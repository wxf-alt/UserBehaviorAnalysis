import OrderTimeout.{OrderEvent, OrderResult}
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.{Configuration, RestOptions}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

/**
 * @Auther: wxf
 * @Date: 2022/11/29 17:12:53
 * @Description: OrderTimeoutProcess
 * @Version 1.0.0
 */
object OrderTimeoutProcess {
  def main(args: Array[String]): Unit = {
    val conf: Configuration = new Configuration()
    conf.setInteger(RestOptions.PORT, 8081)
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf)
    env.setParallelism(1)
    // 设置时间语义 事件时间语义
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val inputPath: String = OrderTimeout.getClass.getResource("OrderLog.csv").getPath
    val inputStream: DataStream[String] = env.readTextFile(inputPath).uid("input")

    val mapStream: DataStream[OrderEvent] = inputStream.map(x => {
      val str: Array[String] = x.split(",")
      OrderEvent(str(0).toLong, str(1), str(2), str(3).toLong * 1000L)
    }).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[OrderEvent](Time.seconds(3)) {
      override def extractTimestamp(element: OrderEvent): Long = element.timeStamp
    })

    val orderResultStream: DataStream[OrderResult] = mapStream.keyBy(_.orderId)
      .process(new OrderTimeProcessFunction())

    orderResultStream.print("payed：")
    orderResultStream.getSideOutput(new OutputTag[OrderResult]("timeout")).print("timeout：")

    env.execute("OrderTimeoutProcess")
  }

  // 自定义 ProcessFunction 做精细化流程控制
  // 主流输出 支付成功订单; 侧输出流 输出超时报警订单
  class OrderTimeProcessFunction extends KeyedProcessFunction[Long, OrderEvent, OrderResult] {

    // 定义状态 用来保存 是否来过create和pay事件的标识位，以及定时器时间戳
    lazy val isPayedState: ValueState[Boolean] = getRuntimeContext.getState(new ValueStateDescriptor[Boolean]("is-payed", classOf[Boolean]))
    lazy val isCreatedState: ValueState[Boolean] = getRuntimeContext.getState(new ValueStateDescriptor[Boolean]("is-create", classOf[Boolean]))
    lazy val timerTsState: ValueState[Long] = getRuntimeContext.getState(new ValueStateDescriptor[Long]("timer-ts", classOf[Long]))
    lazy val orderTimeoutOutputTag: OutputTag[OrderResult] = new OutputTag[OrderResult]("timeout")

    override def processElement(value: OrderEvent, ctx: KeyedProcessFunction[Long, OrderEvent, OrderResult]#Context, out: Collector[OrderResult]): Unit = {
      // 获取当前状态
      val isPayed: Boolean = isPayedState.value()
      val isCreated: Boolean = isCreatedState.value()
      val timerTs: Long = timerTsState.value()

      // 判断当前事件类型。pay-create
      // 情况1：来的是create，需要判断之前是否有pay来过
      if (value.eventTpye == "create") {
        // 如果 来过pay；代表匹配成功
        if (isPayed) {
          out.collect(OrderResult(value.orderId, "payed successfully"))
          // 清空状态
          isPayedState.clear()
          timerTsState.clear()
          ctx.timerService().deleteEventTimeTimer(timerTs)
        } else { // 如果没有pay。注册定时器 15分钟。开始等待
          val ts: Long = value.timeStamp + 15 * 60 * 1000L
          ctx.timerService().registerEventTimeTimer(ts)
          // 更新状态
          timerTsState.update(ts)
          isCreatedState.update(true)
        }
      } else if (value.eventTpye == "pay") { // 情况2：来的是pay，需要继续判断是否来过create
        // 如果 create 已经来过。匹配成功；还需要判断间隔时间是否超过15分钟
        if (isCreated) {
          // 如果没有超时 正常输出结果
          if (value.timeStamp < timerTs) {
            out.collect(OrderResult(value.orderId, "payed successfully"))
          } else {
            // 如果已经超时 输出到侧输出流
            ctx.output(orderTimeoutOutputTag, OrderResult(value.orderId, "payed but already timeout"))
          }
          // 不论那种情况 都已经输出; 需要清空状态
          isCreatedState.clear()
          timerTsState.clear()
          ctx.timerService().deleteEventTimeTimer(timerTs)
        }
        // 如果create没有来。需要等待乱序数据到来。注册一个当前时间戳的定时器
        else {
          val ts: Long = value.timeStamp
          ctx.timerService().registerEventTimeTimer(ts)
          // 更新状态
          timerTsState.update(ts)
          isPayedState.update(true)
        }
      }


    }

    override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, OrderEvent, OrderResult]#OnTimerContext, out: Collector[OrderResult]): Unit = {
      // 定时器触发 需要判断是那种情况
      if (isPayedState.value()) {
        // 如果 pay 过。那就说明 create 没有来; 可能出现数据异常情况
        ctx.output(orderTimeoutOutputTag, OrderResult(ctx.getCurrentKey, "already payed but not found create log"))
      } else { // 如果没有 pay 过; 那么15分钟超时 报警
        ctx.output(orderTimeoutOutputTag, OrderResult(ctx.getCurrentKey, "order timeout"))
      }
      // 清空状态
      isPayedState.clear()
      isCreatedState.clear()
    }

  }


}
