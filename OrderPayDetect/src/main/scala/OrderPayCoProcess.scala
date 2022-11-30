import OrderTimeout.OrderEvent
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.{Configuration, RestOptions}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.co.{CoProcessFunction, KeyedCoProcessFunction}
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

/**
 * @Auther: wxf
 * @Date: 2022/11/29 19:13:50
 * @Description: OrderPayCoProcess
 * @Version 1.0.0
 */
object OrderPayCoProcess {
  def main(args: Array[String]): Unit = {
    val conf: Configuration = new Configuration()
    conf.setInteger(RestOptions.PORT, 8081)
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf)
    env.setParallelism(1)
    // 设置时间语义 事件时间语义
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val inputPath1: String = OrderTimeout.getClass.getResource("OrderLog.csv").getPath
    val inputStream1: DataStream[String] = env.readTextFile(inputPath1)

    val orderEventStream: DataStream[OrderEvent] = inputStream1.map(x => {
      val str: Array[String] = x.split(",")
      OrderEvent(str(0).toLong, str(1), str(2), str(3).toLong * 1000L)
    }).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[OrderEvent](Time.seconds(3)) {
      override def extractTimestamp(element: OrderEvent): Long = element.timeStamp
    }).filter(_.txId != "") // 只过滤出 pay 事件

    val inputPath2: String = OrderTimeout.getClass.getResource("ReceiptLog.csv").getPath
    val inputStream2: DataStream[String] = env.readTextFile(inputPath2)

    val receiptEventStream: DataStream[ReceiptEvent] = inputStream2.map(x => {
      val str: Array[String] = x.split(",")
      ReceiptEvent(str(0), str(1), str(2).toLong * 1000L)
    }).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[ReceiptEvent](Time.seconds(3)) {
      override def extractTimestamp(element: ReceiptEvent): Long = element.timeStamp
    })

    // 使用 connect 连接两条流
    val resultStream: DataStream[(OrderEvent, ReceiptEvent)] = orderEventStream.connect(receiptEventStream)
      .keyBy(x => x.txId, y => y.txId)
      .process(new OrderPayTxDetectProcess())

    // 定义侧输出流
    val unmatchedPays: OutputTag[OrderEvent] = new OutputTag[OrderEvent]("unmatched-pays")
    val unmatchedReceipts: OutputTag[ReceiptEvent] = new OutputTag[ReceiptEvent]("unmatched-receipts")

    resultStream.print("resultStream：")
    resultStream.getSideOutput(unmatchedPays).print("unmatched-pays")
    resultStream.getSideOutput(unmatchedReceipts).print("unmatched-receipts")

    env.execute("OrderPayCoProcess")
  }

  class OrderPayTxDetectProcess() extends KeyedCoProcessFunction[String, OrderEvent, ReceiptEvent, (OrderEvent, ReceiptEvent)] {

    // 定义两个状态 存储 两条流的数据
    lazy val orderEventState: ValueState[OrderEvent] = getRuntimeContext.getState(new ValueStateDescriptor[OrderEvent]("pay", classOf[OrderEvent]))
    lazy val receiptEventState: ValueState[ReceiptEvent] = getRuntimeContext.getState(new ValueStateDescriptor[ReceiptEvent]("receipt", classOf[ReceiptEvent]))

    lazy val unmatchedPays: OutputTag[OrderEvent] = new OutputTag[OrderEvent]("unmatched-pays")
    lazy val unmatchedReceipts: OutputTag[ReceiptEvent] = new OutputTag[ReceiptEvent]("unmatched-receipts")

    override def processElement1(value: OrderEvent, ctx: KeyedCoProcessFunction[String, OrderEvent, ReceiptEvent, (OrderEvent, ReceiptEvent)]#Context, out: Collector[(OrderEvent, ReceiptEvent)]): Unit = {
      val receiptEvent: ReceiptEvent = receiptEventState.value()
      if (receiptEvent != null) {
        out.collect(value, receiptEvent)
        receiptEventState.clear()
      } else { // 如果 receiptEvent 没有数据到来; 将pay存入状态; 定义时间戳 等待
        orderEventState.update(value)
        ctx.timerService().registerEventTimeTimer(value.timeStamp + 5000L)
      }
    }

    override def processElement2(value: ReceiptEvent, ctx: KeyedCoProcessFunction[String, OrderEvent, ReceiptEvent, (OrderEvent, ReceiptEvent)]#Context, out: Collector[(OrderEvent, ReceiptEvent)]): Unit = {
      val orderEvent: OrderEvent = orderEventState.value()
      if (orderEvent != null) {
        out.collect(orderEvent, value)
        orderEventState.clear()
      } else { // 如果 orderEventState 没有数据到来; 将 ReceiptEvent 存入状态; 定义时间戳 等待
        receiptEventState.update(value)
        ctx.timerService().registerEventTimeTimer(value.timeStamp + 3000L)
      }
    }

    override def onTimer(timestamp: Long, ctx: KeyedCoProcessFunction[String, OrderEvent, ReceiptEvent, (OrderEvent, ReceiptEvent)]#OnTimerContext, out: Collector[(OrderEvent, ReceiptEvent)]): Unit = {
      // 如果 pay 不为null 说明 receipt 没来;(如果receipt来了那么一定会clear orderEventState)
      if (orderEventState.value() != null) {
        ctx.output(unmatchedPays, orderEventState.value())
      }
      if (receiptEventState.value() != null) {
        ctx.output(unmatchedReceipts, receiptEventState.value())
      }

      // 清空状态
      orderEventState.clear()
      receiptEventState.clear()

    }

  }

  // 定义到账数据的样例类
  case class ReceiptEvent(txId: String, payChannel: String, timeStamp: Long)

}
