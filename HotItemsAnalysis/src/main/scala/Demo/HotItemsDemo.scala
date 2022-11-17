package Demo

import java.sql.Timestamp
import java.util.Properties

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.api.java.tuple.{Tuple, Tuple1}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.util.Collector

import scala.collection.mutable.ListBuffer

/**
 * Copyright (c) 2018-2028 尚硅谷 All Rights Reserved
 *
 * Project: UserBehaviorAnalysis
 * Package: com.atguigu.hotitems_analysis
 * Version: 1.0
 *
 * Created by wushengran on 2020/4/25 15:39
 */

// 定义输入数据的样例类
case class UserBehavior(userId: Long, itemId: Long, categoryId: Int, behavior: String, timestamp: Long)

// 定义窗口聚合结果的样例类
case class ItemViewCount(itemId: Long, windowEnd: Long, count: Long)

object HotItemsDemo {
  def main(args: Array[String]): Unit = {
    // 创建一个流处理执行环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    // 从文件读取数据
    //    val inputPath: String = HotItemsDemo.getClass.getResource("UserBehavior.csv").getPath
    //    val inputStream: DataStream[String] = env.readTextFile("E:\\A_data\\3.code\\UserBehaviorAnalysis\\HotItemsAnalysis\\src\\main\\resources\\UserBehavior.csv")

    // 从kafka读取数据
    val properties = new Properties()
    properties.setProperty("bootstrap.servers", "hadoop104:9092")
    properties.setProperty("group.id", "consumer-group")
    properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    //    properties.setProperty("auto.offset.reset", "latest")
    val kafkaConsumer: FlinkKafkaConsumer[String] = new FlinkKafkaConsumer[String]("hotitems", new SimpleStringSchema(), properties)
    kafkaConsumer.setCommitOffsetsOnCheckpoints(true)
    kafkaConsumer.setStartFromGroupOffsets()
    val inputStream: DataStream[String] = env.addSource(kafkaConsumer)

    // 将数据转换成样例类类型，并且提取timestamp定义watermark
    val dataStream: DataStream[UserBehavior] = inputStream
      .map(data => {
        val dataArray = data.split(",")
        UserBehavior(dataArray(0).toLong, dataArray(1).toLong, dataArray(2).toInt, dataArray(3), dataArray(4).toLong)
      })
      .assignAscendingTimestamps(_.timestamp * 1000L)

    // 对数据进行转换，过滤出pv行为，开窗聚合统计个数
    val aggStream: DataStream[ItemViewCount] = dataStream
      .filter(_.behavior == "pv") // 过滤pv行为
      .keyBy("itemId") // 按照itemId分组
      .timeWindow(Time.hours(1), Time.minutes(5)) // 定义滑动窗口
      .aggregate(new CountAgg(), new ItemCountWindowResult())

    // 对窗口聚合结果按照窗口进行分组，并做排序取TopN输出
    val resultStream: DataStream[String] = aggStream
      .keyBy("windowEnd")
      .process(new TopNHotItems(5))

    resultStream.print()

    env.execute("hot items job")
  }
}

// 自定义预聚合函数，来一条数据就加1
class CountAgg() extends AggregateFunction[UserBehavior, Long, Long] {
  override def add(value: UserBehavior, accumulator: Long): Long = accumulator + 1

  override def createAccumulator(): Long = 0L

  override def getResult(accumulator: Long): Long = accumulator

  override def merge(a: Long, b: Long): Long = a + b
}

// 扩展：自定义求平均值的聚合函数，状态为（sum，count）
class AvgAgg() extends AggregateFunction[UserBehavior, (Long, Int), Double] {
  override def add(value: UserBehavior, accumulator: (Long, Int)): (Long, Int) =
    (accumulator._1 + value.timestamp, accumulator._2 + 1)

  override def createAccumulator(): (Long, Int) = (0L, 0)

  override def getResult(accumulator: (Long, Int)): Double = accumulator._1 / accumulator._2.toDouble

  override def merge(a: (Long, Int), b: (Long, Int)): (Long, Int) =
    (a._1 + b._1, a._2 + b._2)
}

// 自定义窗口函数，结合window信息包装成样例类
class ItemCountWindowResult() extends WindowFunction[Long, ItemViewCount, Tuple, TimeWindow] {
  override def apply(key: Tuple, window: TimeWindow, input: Iterable[Long], out: Collector[ItemViewCount]): Unit = {
    val itemId = key.asInstanceOf[Tuple1[Long]].f0
    val windowEnd = window.getEnd
    val count = input.iterator.next()
    out.collect(ItemViewCount(itemId, windowEnd, count))
  }
}


// 自定义 KeyedProcessFunction
class TopNHotItems(n: Int) extends KeyedProcessFunction[Tuple, ItemViewCount, String] {
  // 定义一个ListState，用来保存当前窗口所有的count结果
  lazy val itemCountListState: ListState[ItemViewCount] = getRuntimeContext.getListState(new ListStateDescriptor[ItemViewCount]("itemcount-list", classOf[ItemViewCount]))

  override def processElement(value: ItemViewCount, ctx: KeyedProcessFunction[Tuple, ItemViewCount, String]#Context, out: Collector[String]): Unit = {
    // 每来一条数据，就把它保存到状态中
    itemCountListState.add(value)
    // 注册定时器，在 windowEnd + 100 触发
    ctx.timerService().registerEventTimeTimer(value.windowEnd + 100)
  }

  // 定时器触发时，从状态中取数据，然后排序输出
  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Tuple, ItemViewCount, String]#OnTimerContext, out: Collector[String]): Unit = {
    // 先把状态中的数据提取到一个ListBuffer中
    val allItemCountList: ListBuffer[ItemViewCount] = ListBuffer()
    import scala.collection.JavaConversions._
    for (itemCount <- itemCountListState.get()) {
      allItemCountList += itemCount
    }

    // 按照count值大小排序，取TopN
    val sortedItemCountList = allItemCountList.sortBy(_.count)(Ordering.Long.reverse).take(n)

    // 清除状态
    itemCountListState.clear()

    // 将排名信息格式化成String，方便监控显示
    val result: StringBuilder = new StringBuilder
    result.append("时间：").append(new Timestamp(timestamp - 100)).append("\n")
    // 遍历sorted列表，输出TopN信息
    for (i <- sortedItemCountList.indices) {
      // 获取当前商品的count信息
      val currentItemCount = sortedItemCountList(i)
      result.append("Top").append(i + 1).append(":")
        .append(" 商品ID=").append(currentItemCount.itemId)
        .append(" 访问量=").append(currentItemCount.count)
        .append("\n")
    }
    result.append("==============================\n\n")

    // 控制输出频率
    Thread.sleep(1000)
    out.collect(result.toString())
  }
}