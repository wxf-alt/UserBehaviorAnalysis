import java.sql.Timestamp
import java.util.UUID

import org.apache.flink.configuration.{Configuration, RestOptions}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.source.{RichParallelSourceFunction, SourceFunction}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import scala.util.Random

/**
 * @Auther: wxf
 * @Date: 2022/11/21 17:09:30
 * @Description: AppMarketingByChannel  App市场推广统计
 * @Version 1.0.0
 */

// 输入数据样例类
case class MarketUserBehavior(userId: String, behavior: String, channel: String, timeStamp: Long)

// 输出数据样例类
case class MarketViewCount(windowStart: String, windowEnd: String, channel: String, behavior: String, count: Long)

object AppMarketingByChannel {
  def main(args: Array[String]): Unit = {
    val conf: Configuration = new Configuration()
    conf.setInteger(RestOptions.PORT, 8081)
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf)
    env.setParallelism(1)
    // 设置时间语义 事件时间语义
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    // 数据源
    val inputSteam: DataStream[MarketUserBehavior] = env.addSource(SimulateMarketEventSource(500000))
      .assignAscendingTimestamps(x => x.timeStamp)

    val markCountStream: DataStream[MarketViewCount] = inputSteam
      .filter(_.behavior != "UNINSTALL")
      .keyBy(x => (x.behavior, x.channel))
      .timeWindow(Time.hours(1), Time.seconds(5))
      .process(new MarketCountByChannel())

    markCountStream.print("markCountStream：")
    env.execute("AppMarketingByChannel")
  }
}

class MarketCountByChannel extends ProcessWindowFunction[MarketUserBehavior, MarketViewCount, (String, String), TimeWindow] {
  override def process(key: (String, String), context: Context, elements: Iterable[MarketUserBehavior], out: Collector[MarketViewCount]): Unit = {
    val windowStart: String = new Timestamp(context.window.getStart).toString
    val windowEnd: String = new Timestamp(context.window.getEnd).toString
    val channel: String = key._2
    val behavior: String = key._1
    val count: Int = elements.size
    out.collect(MarketViewCount(windowStart, windowEnd, channel, behavior, count))
  }
}


// 自定义数据源
case class SimulateMarketEventSource(maxCounts: Long) extends RichParallelSourceFunction[MarketUserBehavior] {

  var runFlag: Boolean = true

  // 定义随机数生成器
  val rand: Random = Random
  // 可选 用户行为:Behavior 和 渠道:channel 的集合
  val behaviorSet: Seq[String] = Seq("CLICK", "DOWNLOAD", "INSTALL", "UNINSTALL")
  val channelSet: Seq[String] = Seq("appstore", "huaweiStore", "weibo", "wechat")

  override def run(ctx: SourceFunction.SourceContext[MarketUserBehavior]): Unit = {
    var count: Long = 0L
    while (runFlag && count < maxCounts) {
      val userId: String = UUID.randomUUID().toString
      val behavior: String = behaviorSet(rand.nextInt(behaviorSet.size))
      val channel: String = channelSet(rand.nextInt(channelSet.size))
      val ts: Long = System.currentTimeMillis()
      ctx.collect(MarketUserBehavior(userId, behavior, channel, ts))
      count += 1
      Thread.sleep(rand.nextInt(10) * 100)
    }

  }

  override def cancel(): Unit = runFlag = false

}