import java.lang
import java.sql.Timestamp

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.assigners.{SlidingEventTimeWindows}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

/**
 * @Auther: wxf
 * @Date: 2022/11/15 15:19:12
 * @Description: HotItems  每隔5分钟输出最近一小时内 点击量 最多的前N个商品。
 * @Version 1.0.0
 */
object HotItems {

  // 定义输入数据的样例类    用户,商品，类别，行为，时间(秒)
  case class UserBehavior(userId: Long, itemId: Long, categoryId: Int, behavior: String, timeStamp: Long)
  // 定义窗口聚合结果的样例类
  case class ItemViewCount(itemId: Long, windowEnd: Long, count: Long)

  def main(args: Array[String]): Unit = {

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration())
    env.setParallelism(1)
    // 设置时间语义 事件时间语义
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val inputPath: String = HotItems.getClass.getResource("UserBehavior.csv").getPath
    val inputStream: DataStream[String] = env.readTextFile(inputPath)

    val mapStream: DataStream[UserBehavior] = inputStream.map(x => {
      val str: Array[String] = x.split(",")
      UserBehavior(str(0).toLong, str(1).toLong, str(2).toInt, str(3), str(4).toLong)
    })
      // 设置 waterMark 和 定义时间戳
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[UserBehavior](Time.seconds(1)) {
        override def extractTimestamp(element: UserBehavior): Long = element.timeStamp * 1000L
      })

    // 过滤出 pv 行为，开窗聚合统计个数 计算窗口中出每个商品的点击量
    val windowStream: DataStream[ItemViewCount] = mapStream
      .filter(_.behavior == "pv")
      .keyBy(_.itemId) // 过滤 pv 行为
      .window(SlidingEventTimeWindows.of(Time.hours(1), Time.minutes(5)))
      .aggregate(CountAgg(), ItemCountWindowResult())

    val result: DataStream[(String, Long, Long, Int)] = windowStream
      .keyBy(_.windowEnd)
      .process(TopNKeyedProcessFuncrion(5))

    result.print("result：")

    env.execute("HotItems")
  }

  // key -> 商品ID 可以在 全量窗口函数中获取到
  case class CountAgg() extends AggregateFunction[UserBehavior, Long, Long] {
    override def createAccumulator(): Long = 0L

    override def add(value: UserBehavior, accumulator: Long): Long = accumulator + 1

    override def getResult(accumulator: Long): Long = accumulator

    override def merge(a: Long, b: Long): Long = a + b
  }

  case class ItemCountWindowResult() extends ProcessWindowFunction[Long, ItemViewCount, Long, TimeWindow] {
    override def process(key: Long, context: Context, elements: Iterable[Long], out: Collector[ItemViewCount]): Unit = {
      val count: Long = elements.iterator.next()
      out.collect(ItemViewCount(key, context.window.getEnd(), count))
    }
  }

  case class TopNKeyedProcessFuncrion(rownum: Int) extends KeyedProcessFunction[Long, ItemViewCount, (String, Long, Long, Int)] {
    // 创建一个列表状态 将输入的所有数据 进行存储
    lazy val itemCountListState: ListState[ItemViewCount] = getRuntimeContext.getListState(new ListStateDescriptor[ItemViewCount]("list-elem", classOf[ItemViewCount]))

    override def processElement(value: ItemViewCount, ctx: KeyedProcessFunction[Long, ItemViewCount, (String, Long, Long, Int)]#Context, out: Collector[(String, Long, Long, Int)]): Unit = {
      // 将数据 添加到 listState 状态中
      itemCountListState.add(value)
      // 注册定时器 在 windowEnd + 1 时 触发
      ctx.timerService().registerEventTimeTimer(value.windowEnd + 1)
    }

    override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, ItemViewCount, (String, Long, Long, Int)]#OnTimerContext, out: Collector[(String, Long, Long, Int)]): Unit = {
      // 从状态中获取 所有数据
      val itemViewCounts: lang.Iterable[ItemViewCount] = itemCountListState.get()
      import scala.collection.convert.wrapAll._
      val list: List[ItemViewCount] = itemViewCounts.toList
      // 降序排序
      val sortedItemCountList: List[ItemViewCount] = list.sortBy(_.count)(Ordering.Long.reverse).take(rownum)
      //      list.sortWith(_.count - _.count > 0)

      // 清空状态
      itemCountListState.clear()

      // 输出
      for (elem <- 0 until sortedItemCountList.size) {
        val viewCount: ItemViewCount = sortedItemCountList(elem)
        Thread.sleep(1000)
        //        val timeStamp: String = new Timestamp(timestamp - 1).toString
        val timeStamp: String = new Timestamp(viewCount.windowEnd).toString
        out.collect(timeStamp, viewCount.itemId, viewCount.count, elem + 1)
      }

    }

  }

}