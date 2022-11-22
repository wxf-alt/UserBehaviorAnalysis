import org.apache.flink.api.common.functions.{AggregateFunction, RichMapFunction}
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.{Configuration, RestOptions}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import scala.util.Random

/**
 * @Auther: wxf
 * @Date: 2022/11/17 17:31:00
 * @Description: PageView  统计 PV
 * @Version 1.0.0
 */
object PageView {
  def main(args: Array[String]): Unit = {
    val conf: Configuration = new Configuration()
    conf.setInteger(RestOptions.PORT, 8081)
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf)
    env.setParallelism(4)
    // 设置时间语义 事件时间语义
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val inputStream: DataStream[String] = env.readTextFile("E:\\A_data\\3.code\\UserBehaviorAnalysis\\HotItemsAnalysis\\src\\main\\resources\\UserBehavior.csv")
    //    val inputStream: DataStream[String] = env.socketTextStream("localhost", 6666)

    val mapStream: DataStream[UserBehavior] = inputStream.map(x => {
      val str: Array[String] = x.split(",")
      UserBehavior(str(0).toLong, str(1).toLong, str(2).toInt, str(3), str(4).toLong)
    }) // 设置 waterMark 和 定义时间戳
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[UserBehavior](Time.seconds(1)) {
        override def extractTimestamp(element: UserBehavior): Long = element.timestamp * 1000L
      })

    //    // 过滤出 pv 行为，开窗聚合统计个数 计算窗口中出每个商品的点击量
    //    val windowStream: DataStream[PvCount] = mapStream
    //      .filter(_.behavior == "pv")
    //      // 为每一条数据分配同一个 key
    //      .keyBy(x => true)
    //      .timeWindow(Time.hours(1))
    //      .aggregate(new PvCountAgg(), new PvCountResult())
    //
    //    windowStream.print("windowStream：")

    // 数据倾斜优化
    val windowStream: DataStream[PvCount] = mapStream
      .filter(_.behavior == "pv")
      .map(new MyMapper())
      // 为每一条数据分配同一个 key
      .keyBy(_._1)
      .timeWindow(Time.hours(1))
      .aggregate(new PvCountAgg1(), new PvCountResult1())

    val pvTotalStream: DataStream[PvCount] = windowStream
      .keyBy(_.windowEnd)
      .process(new TotalPvCountResult())

    pvTotalStream.print("pvTotalStream：")

     env.execute("PageView")
  }

  // 随机生成 key
  class MyMapper extends RichMapFunction[UserBehavior, (String, Long)] {
    lazy val subtask: Int = getRuntimeContext.getIndexOfThisSubtask

    override def map(value: UserBehavior): (String, Long) = {
      //      (Random.nextString(10), 1)
      (subtask.toString, 1)
    }
  }

  // 自定义ProcessFunction，将聚合结果按窗口进行合并
  class TotalPvCountResult extends KeyedProcessFunction[Long, PvCount, PvCount] {
    // 定义状态 保存结果和
    lazy val totalCountState: ValueState[Long] = getRuntimeContext.getState(new ValueStateDescriptor[Long]("total-count", classOf[Long]))

    override def processElement(value: PvCount, ctx: KeyedProcessFunction[Long, PvCount, PvCount]#Context, out: Collector[PvCount]): Unit = {
      val valueTmp: Long = totalCountState.value()
      totalCountState.update(valueTmp + value.count)
      ctx.timerService().registerEventTimeTimer(value.windowEnd + 1)
    }

    override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, PvCount, PvCount]#OnTimerContext, out: Collector[PvCount]): Unit = {
      val result: Long = totalCountState.value()
      //      out.collect(PvCount(ctx.getCurrentKey, result))
      out.collect(PvCount(timestamp - 1, result))
      // 清空状态
      totalCountState.clear()
    }

  }

  class PvCountAgg extends AggregateFunction[UserBehavior, Long, Long] {
    override def createAccumulator(): Long = 0L

    override def add(value: UserBehavior, accumulator: Long): Long = accumulator + 1L

    override def getResult(accumulator: Long): Long = accumulator

    override def merge(a: Long, b: Long): Long = a + b
  }

  class PvCountResult extends ProcessWindowFunction[Long, PvCount, Boolean, TimeWindow] {
    override def process(key: Boolean, context: Context, elements: Iterable[Long], out: Collector[PvCount]): Unit = {
      val windowEnd: Long = context.window.getEnd
      Thread.sleep(1000)
      out.collect(PvCount(windowEnd, elements.head))
    }
  }

  class PvCountAgg1 extends AggregateFunction[(String, Long), Long, Long] {
    override def createAccumulator(): Long = 0L

    override def add(value: (String, Long), accumulator: Long): Long = accumulator + 1L

    override def getResult(accumulator: Long): Long = accumulator

    override def merge(a: Long, b: Long): Long = a + b
  }

  class PvCountResult1 extends ProcessWindowFunction[Long, PvCount, String, TimeWindow] {
    override def process(key: String, context: Context, elements: Iterable[Long], out: Collector[PvCount]): Unit = {
      val windowEnd: Long = context.window.getEnd
      out.collect(PvCount(windowEnd, elements.head))
    }
  }

  // 定义输入输出的样例类
  case class UserBehavior(userId: Long, itemId: Long, categoryId: Int, behavior: String, timestamp: Long)

  case class PvCount(windowEnd: Long, count: Long)

}