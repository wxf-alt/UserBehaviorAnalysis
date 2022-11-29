import java.util

import OrderTimeout.{OrderEvent, OrderResult}
import org.apache.flink.cep.{PatternSelectFunction, PatternTimeoutFunction}
import org.apache.flink.cep.scala.{CEP, PatternStream}
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.configuration.{Configuration, RestOptions}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

/**
 * @Auther: wxf
 * @Date: 2022/11/25 16:18:38
 * @Description: OrderTimeout
 * @Version 1.0.0
 */
object OrderTimeout {
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

    // 定义要匹配的事件的模式
    val orderPayPatten: Pattern[OrderEvent, OrderEvent] = Pattern
      .begin[OrderEvent]("create").where(_.eventTpye == "create")
      .followedBy("pay").where(_.eventTpye == "pay")
      .within(Time.minutes(15))

    val pattenStream: PatternStream[OrderEvent] = CEP.pattern(mapStream.keyBy(_.orderId), orderPayPatten)

    // 定义侧输出流 用来返回超时事件
    val orderTimeoutOutputTag: OutputTag[OrderResult] = new OutputTag[OrderResult]("order-timeOut")
    // 使用超时事件 判断订单超时
    val result: DataStream[OrderResult] = pattenStream.select(orderTimeoutOutputTag, new OrderTimeoutSelect(), new OrderPaySelect())

    val timeOutEventStream: DataStream[OrderResult] = result.getSideOutput(orderTimeoutOutputTag)

    result.print("result：")
    timeOutEventStream.print("timeOutEventStream：")

    env.execute("OrderTimeout")
  }

  class OrderTimeoutSelect extends PatternTimeoutFunction[OrderEvent, OrderResult] {
    override def timeout(pattern: util.Map[String, util.List[OrderEvent]], timeoutTimestamp: Long): OrderResult = {
      val createEvent: OrderEvent = pattern.get("create").get(0)
      OrderResult(createEvent.orderId, "订单超时")
    }
  }

  class OrderPaySelect extends PatternSelectFunction[OrderEvent, OrderResult] {
    override def select(pattern: util.Map[String, util.List[OrderEvent]]): OrderResult = {
      val createEvent: OrderEvent = pattern.get("create").get(0)
      OrderResult(createEvent.orderId, "订单支付成功")
    }
  }


  // 输入输出样例类
  case class OrderEvent(orderId: Long, eventTpye: String, txId: String, timeStamp: Long)

  case class OrderResult(orderId: Long, resultMsg: String)

}
