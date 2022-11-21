import PageView.UserBehavior
import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.configuration.{Configuration, RestOptions}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.{AllWindowFunction, ProcessAllWindowFunction, ProcessWindowFunction}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

/**
 * @Auther: wxf
 * @Date: 2022/11/17 19:59:44
 * @Description: UniqueVisitor
 * @Version 1.0.0
 */
object UniqueVisitor {
  def main(args: Array[String]): Unit = {

    val conf: Configuration = new Configuration()
    conf.setInteger(RestOptions.PORT, 8081)
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf)
    env.setParallelism(1)
    // 设置时间语义 事件时间语义
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val inputStream: DataStream[String] = env.readTextFile("E:\\A_data\\3.code\\UserBehaviorAnalysis\\HotItemsAnalysis\\src\\main\\resources\\UserBehavior.csv")

    val mapStream: DataStream[UserBehavior] = inputStream.map(x => {
      val str: Array[String] = x.split(",")
      UserBehavior(str(0).toLong, str(1).toLong, str(2).toInt, str(3), str(4).toLong)
    }) // 设置 waterMark 和 定义时间戳
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[UserBehavior](Time.seconds(1)) {
        override def extractTimestamp(element: UserBehavior): Long = element.timestamp * 1000L
      })

    //    val windowStream: DataStream[UvCount] = mapStream
    //      .filter(_.behavior == "pv")
    //      // 为每一条数据分配同一个 key
    //      .timeWindowAll(Time.hours(1))
    //      .apply(new UcVountResult())

    val windowStream: DataStream[UvCount] = mapStream
      .filter(_.behavior == "pv")
      .timeWindowAll(Time.hours(1))
      .aggregate(new UvCountAff(), new UvCountResultProcessFunction())

    windowStream.print("windowStream：")
    env.execute("UniqueVisitor")

  }

  case class UvCount(windowEnd: Long, count: Long)




  // UV 增量去重函数
  class UvCountAff() extends AggregateFunction[UserBehavior, Set[Long], Long] {
    override def createAccumulator(): Set[Long] = Set[Long]()

    override def add(value: UserBehavior, accumulator: Set[Long]): Set[Long] = accumulator + value.userId

    override def getResult(accumulator: Set[Long]): Long = accumulator.size

    override def merge(a: Set[Long], b: Set[Long]): Set[Long] = a ++ b
  }

  // UV 全窗口函数
  class UvCountResultProcessFunction() extends ProcessAllWindowFunction[Long, UvCount, TimeWindow] {
    override def process(context: Context, elements: Iterable[Long], out: Collector[UvCount]): Unit = {
      out.collect(UvCount(context.window.getEnd, elements.head))
    }
  }

  class UcVountResult extends AllWindowFunction[UserBehavior, UvCount, TimeWindow] {
    override def apply(window: TimeWindow, input: Iterable[UserBehavior], out: Collector[UvCount]): Unit = {
      var idSet: Set[Long] = Set[Long]()
      for (elem <- input) {
        idSet += elem.userId
      }
      out.collect(UvCount(window.getEnd, idSet.size))
    }
  }

}