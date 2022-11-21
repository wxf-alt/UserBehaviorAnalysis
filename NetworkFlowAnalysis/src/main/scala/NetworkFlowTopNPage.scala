import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.state.{MapState, MapStateDescriptor}
import org.apache.flink.configuration.{Configuration, RestOptions}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util
import java.util.Map
import scala.collection.mutable.ListBuffer

/**
 * @Auther: wxf
 * @Date: 2022/11/17 14:37:56
 * @Description: NetworkFlowTopNPage  基于 服务器log 统计热门页面浏览量
 * @Version 1.0.0
 */
object NetworkFlowTopNPage {
  def main(args: Array[String]): Unit = {
    val conf: Configuration = new Configuration()
    //    conf.setInteger(RestOptions.PORT, 8081)
    conf.setString(RestOptions.BIND_PORT, "8081-8089")
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf)
    env.setParallelism(1)
    // 设置时间语义 事件时间语义
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    //    val inputPath: String = NetworkFlowTopNPage.getClass.getResource("apache.log").getPath
    //    val inputStream: DataStream[String] = env.readTextFile(inputPath)
    val inputStream: DataStream[String] = env.socketTextStream("localhost", 6666)

    val mapStream: DataStream[ApacheLogEvent] = inputStream.map(x => {
      val str: Array[String] = x.split(" ")
      val format: SimpleDateFormat = new SimpleDateFormat("dd/MM/yyyy:HH:mm:ss")
      val eventTime: Long = format.parse(str(3)).getTime
      ApacheLogEvent(str(0), str(1), eventTime, str(5), str(6))
    }) // 设置 waterMark 和 定义时间戳
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[ApacheLogEvent](Time.seconds(1)) {
        override def extractTimestamp(element: ApacheLogEvent): Long = element.eventTime
      })

    val lateOutputTag: OutputTag[ApacheLogEvent] = new OutputTag[ApacheLogEvent]("late-data")

    // 过滤出 pv 行为，开窗聚合统计个数 计算窗口中出每个商品的点击量
    val windowStream: DataStream[PageViewCount] = mapStream
      .keyBy(_.url) // 计算 pv 页面访问量
      .window(SlidingEventTimeWindows.of(Time.minutes(10), Time.seconds(5)))
      .allowedLateness(Time.minutes(1))
      .sideOutputLateData(lateOutputTag)
      .aggregate(PageCountAgg(), PageCountWindowResult())

    // 迟到数据
    val lateDataStream: DataStream[ApacheLogEvent] = windowStream.getSideOutput(lateOutputTag)

    val result: DataStream[String] = windowStream
      .keyBy(_.windowEnd)
      .process(TopNKeyedProcessFuncrion(3))

    lateDataStream.print("迟到数据：")
    result.print("result：")

    env.execute("NetworkFlowTopNPage")

  }

  // 输入样例类
  case class ApacheLogEvent(ip: String, userId: String, eventTime: Long, method: String, url: String)

  // 聚合结果样例类
  case class PageViewCount(url: String, windowEnd: Long, count: Long)


  // key -> 商品ID 可以在 全量窗口函数中获取到
  case class PageCountAgg() extends AggregateFunction[ApacheLogEvent, Long, Long] {
    override def createAccumulator(): Long = 0L

    override def add(value: ApacheLogEvent, accumulator: Long): Long = accumulator + 1

    override def getResult(accumulator: Long): Long = accumulator

    override def merge(a: Long, b: Long): Long = a + b
  }

  case class PageCountWindowResult() extends ProcessWindowFunction[Long, PageViewCount, String, TimeWindow] {
    override def process(key: String, context: Context, elements: Iterable[Long], out: Collector[PageViewCount]): Unit = {
      out.collect(PageViewCount(key, context.window.getEnd(), elements.head))
    }
  }

  case class TopNKeyedProcessFuncrion(n: Int) extends KeyedProcessFunction[Long, PageViewCount, String] {
    // 定义MapState保存所有聚合结果
    lazy val pageCountMapState: MapState[String, Long] = getRuntimeContext.getMapState(new MapStateDescriptor[String, Long]("pagecount-map", classOf[String], classOf[Long]))

    override def processElement(value: PageViewCount, ctx: KeyedProcessFunction[Long, PageViewCount, String]#Context, out: Collector[String]): Unit = {
      pageCountMapState.put(value.url, value.count)
      ctx.timerService().registerEventTimeTimer(value.windowEnd + 1)
      ctx.timerService().registerEventTimeTimer(value.windowEnd + 60 * 1000L)
    }

    // 等到数据都到齐，从状态中取出，排序输出
    override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, PageViewCount, String]#OnTimerContext, out: Collector[String]): Unit = {
      if (timestamp == ctx.getCurrentKey + 60 * 1000L) {
        pageCountMapState.clear()
        return
      }

      val allPageCountList: ListBuffer[(String, Long)] = ListBuffer()
      val iter: util.Iterator[Map.Entry[String, Long]] = pageCountMapState.entries().iterator()
      while (iter.hasNext) {
        val entry: Map.Entry[String, Long] = iter.next()
        allPageCountList += ((entry.getKey, entry.getValue))
      }
      val sortedPageCountList: ListBuffer[(String, Long)] = allPageCountList.sortWith(_._2 > _._2).take(n)
      val result: StringBuilder = new StringBuilder
      result.append("时间：").append(new Timestamp(timestamp - 1)).append("\n")
      // 遍历sorted列表，输出TopN信息
      for (i <- sortedPageCountList.indices) {
        // 获取当前商品的count信息
        val currentItemCount: (String, Long) = sortedPageCountList(i)
        result.append("Top").append(i + 1).append(":")
          .append(" 页面url=").append(currentItemCount._1)
          .append(" 访问量=").append(currentItemCount._2)
          .append("\n")
      }
      result.append("==============================\n\n")

      // 控制输出频率
      //      Thread.sleep(1000)
      out.collect(result.toString())
    }

  }

}
