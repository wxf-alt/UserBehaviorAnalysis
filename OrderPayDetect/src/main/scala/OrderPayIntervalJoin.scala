import OrderPayCoProcess.ReceiptEvent
import OrderTimeout.OrderEvent
import org.apache.flink.configuration.{Configuration, RestOptions}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

/**
 * @Auther: wxf
 * @Date: 2022/11/29 21:21:08
 * @Description: OrderPayIntervalJoin
 * @Version 1.0.0
 */
object OrderPayIntervalJoin {
  def main(args: Array[String]): Unit = {
    val conf: Configuration = new Configuration()
    conf.setInteger(RestOptions.PORT, 8081)
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf)
    env.setParallelism(1)
    // 设置时间语义 事件时间语义
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val inputPath1: String = OrderTimeout.getClass.getResource("OrderLog.csv").getPath
    val inputStream1: DataStream[String] = env.readTextFile(inputPath1)

    val inputPath2: String = OrderTimeout.getClass.getResource("ReceiptLog.csv").getPath
    val inputStream2: DataStream[String] = env.readTextFile(inputPath2)

    val orderEventStream: KeyedStream[OrderEvent, String] = inputStream1.map(x => {
      val str: Array[String] = x.split(",")
      OrderEvent(str(0).toLong, str(1), str(2), str(3).toLong * 1000L)
    }).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[OrderEvent](Time.seconds(3)) {
      override def extractTimestamp(element: OrderEvent): Long = element.timeStamp
    }).filter(_.txId != "") // 只过滤出 pay 事件
      .keyBy(_.txId)

    val receiptEventStream: KeyedStream[ReceiptEvent, String] = inputStream2.map(x => {
      val str: Array[String] = x.split(",")
      ReceiptEvent(str(0), str(1), str(2).toLong * 1000L)
    }).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[ReceiptEvent](Time.seconds(3)) {
      override def extractTimestamp(element: ReceiptEvent): Long = element.timeStamp
    }).keyBy(_.txId)

    val resultStream: DataStream[(OrderEvent, ReceiptEvent)] = orderEventStream.intervalJoin(receiptEventStream)
      .between(Time.seconds(-3), Time.seconds(5))
      .process(new OrderPayTxDetectWithJoinProcess())

    resultStream.print("resultStream:")

    env.execute("OrderPayIntervalJoin")
  }

  class OrderPayTxDetectWithJoinProcess extends ProcessJoinFunction[OrderEvent, ReceiptEvent, (OrderEvent, ReceiptEvent)] {
    override def processElement(left: OrderEvent, right: ReceiptEvent, ctx: ProcessJoinFunction[OrderEvent, ReceiptEvent, (OrderEvent, ReceiptEvent)]#Context, out: Collector[(OrderEvent, ReceiptEvent)]): Unit = {
      out.collect((left, right))
    }
  }

}
