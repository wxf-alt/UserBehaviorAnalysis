import java.sql.Timestamp

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.{Configuration, RestOptions}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

/**
 * @Auther: wxf
 * @Date: 2022/11/21 19:17:20
 * @Description: AdAnalysisByProvince   按照用户地理位置划分，统计不同省份用户在一段时间内对广告的点击 + 过滤黑名单
 * @Version 1.0.0
 */
object AdAnalysisByProvince {
  def main(args: Array[String]): Unit = {
    val conf: Configuration = new Configuration()
    conf.setInteger(RestOptions.PORT, 8081)
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf)
    env.setParallelism(1)
    // 设置时间语义 事件时间语义
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val inputPath: String = AdAnalysisByProvince.getClass.getResource("AdClickLog.csv").getPath

    val inputStream: DataStream[AdClickEvent] = env.readTextFile(inputPath)
      .map(x => {
        val str: Array[String] = x.split(",")
        AdClickEvent(str(0).toLong, str(1).toLong, str(2), str(3), str(4).toLong * 1000L)
      })
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[AdClickEvent](Time.seconds(1)) {
        override def extractTimestamp(element: AdClickEvent): Long = element.timeStamo
      })

    // 定义 刷单行为 过滤操作
    val filterBlackListStream: DataStream[AdClickEvent] = inputStream
      .keyBy(x => (x.userId, x.adId))
      .process(new FilterBlackList(100L))

    // 按照 province 分组 开窗聚合
    val windowResult: DataStream[AdCountByProvince] = filterBlackListStream
      .keyBy(_.province)
      .timeWindow(Time.hours(1), Time.seconds(5))
      .aggregate(new AdCountAgg(), new AdCountResult())

    windowResult.print("windowResult：")
    filterBlackListStream.getSideOutput(new OutputTag[BlackListWaring]("blacklist")).print("blacklist：")

    env.execute("AdAnalysisByProvince")
  }

  // 判断用户对广告的点击次数 是否达到上限
  class FilterBlackList(clickCount: Long) extends KeyedProcessFunction[(Long, Long), AdClickEvent, AdClickEvent] {
    // 定义状态，保存当前用户对当前广告的 点击量
    lazy val countState: ValueState[Long] = getRuntimeContext.getState(new ValueStateDescriptor[Long]("count", classOf[Long]))
    // 保存标识位，用来表示用户是否已经在黑名单中
    lazy val isSentState: ValueState[Boolean] = getRuntimeContext.getState(new ValueStateDescriptor[Boolean]("is-sent", classOf[Boolean]))

    override def processElement(value: AdClickEvent, ctx: KeyedProcessFunction[(Long, Long), AdClickEvent, AdClickEvent]#Context, out: Collector[AdClickEvent]): Unit = {
      val count: Long = countState.value()
      val isSent: Boolean = isSentState.value()
      // 如果输入的是 第一个数，那么注册第二天0点的定时器，用与清空状态
      if (count == 0) {
        // 获取到 到达明天 0 点的 毫秒数
        // ctx.timerService().currentProcessingTime() --> 当前的毫秒值
        // (1000 * 60 * 60 * 24) --> 1天的毫秒值    +1 --> 明天
        val ts: Long = (ctx.timerService().currentProcessingTime() / (1000 * 60 * 60 * 24) + 1) * (1000 * 60 * 60 * 24)
        ctx.timerService().registerProcessingTimeTimer(ts)
      }

      // 判断 count 值 是否达到上限，并且之前没有输出给报警信息，那么报警
      if (count >= clickCount && !isSent) {
        ctx.output(new OutputTag[BlackListWaring]("blacklist"), BlackListWaring(value.userId, value.adId, "click over " + clickCount + " times today"))
        isSentState.update(true)
        return
      }

      countState.update(count + 1)
      out.collect(value)
    }

    // 到达 0 点 触发定时器。清空状态
    override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[(Long, Long), AdClickEvent, AdClickEvent]#OnTimerContext, out: Collector[AdClickEvent]): Unit = {
      countState.clear()
      isSentState.clear()
    }

  }

  class AdCountAgg() extends AggregateFunction[AdClickEvent, Long, Long] {
    override def createAccumulator(): Long = 0L

    override def add(value: AdClickEvent, accumulator: Long): Long = accumulator + 1

    override def getResult(accumulator: Long): Long = accumulator

    override def merge(a: Long, b: Long): Long = a + b
  }

  class AdCountResult() extends WindowFunction[Long, AdCountByProvince, String, TimeWindow] {
    override def apply(key: String, window: TimeWindow, input: Iterable[Long], out: Collector[AdCountByProvince]): Unit = {
      val windowEnd: String = new Timestamp(window.getEnd).toString
      out.collect(AdCountByProvince(key, windowEnd, input.last))
    }
  }

  // 定义输入，输出样例类
  // 2315,36237,zhejiang,hangzhou,1511661692
  case class AdClickEvent(userId: Long, adId: Long, province: String, city: String, timeStamo: Long)

  case class AdCountByProvince(province: String, windowEnd: String, count: Long)

  // 侧输出流报警信息样例类
  case class BlackListWaring(userId: Long, adId: Long, msg: String)

}
