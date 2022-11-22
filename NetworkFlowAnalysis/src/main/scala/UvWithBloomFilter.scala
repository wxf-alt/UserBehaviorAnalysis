import java.lang

import PageView.UserBehavior
import UniqueVisitor.UvCount
import org.apache.flink.configuration.{Configuration, RestOptions}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.triggers.{Trigger, TriggerResult}
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector
import redis.clients.jedis.Jedis

/**
 * @Auther: wxf
 * @Date: 2022/11/18 14:45:23
 * @Description: UvWithBloomFilter
 * @Version 1.0.0
 */
object UvWithBloomFilter {
  def main(args: Array[String]): Unit = {
    val conf: Configuration = new Configuration()
    conf.setInteger(RestOptions.PORT, 8081)
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf)
    env.setParallelism(1)
    // 设置时间语义 事件时间语义
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val inputPath: String = "E:\\A_data\\3.code\\UserBehaviorAnalysis\\HotItemsAnalysis\\src\\main\\resources\\UserBehavior.csv"
    val inputStream: DataStream[String] = env.readTextFile(inputPath)

    val mapStream: DataStream[UserBehavior] = inputStream.map(x => {
      val str: Array[String] = x.split(",")
      UserBehavior(str(0).toLong, str(1).toLong, str(2).toInt, str(3), str(4).toLong)
    }) // 设置 waterMark 和 定义时间戳
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[UserBehavior](Time.seconds(1)) {
        override def extractTimestamp(element: UserBehavior): Long = element.timestamp * 1000L
      })

    val windowStream: DataStream[UvCount] = mapStream
      .filter(_.behavior == "pv")
      .map(x => ("uv", x.userId))
      .keyBy(_._1)
      .timeWindow(Time.hours(1))
      .trigger(new MyTrigger())
      .process(new UvCountResultWithBloomFilter())

    windowStream.print("windowStream：")
    env.execute("UvWithBloomFilter")
  }

  class MyTrigger() extends Trigger[(String, Long), TimeWindow] {
    override def onElement(element: (String, Long), timestamp: Long, window: TimeWindow, ctx: Trigger.TriggerContext): TriggerResult = TriggerResult.FIRE_AND_PURGE

    override def onProcessingTime(time: Long, window: TimeWindow, ctx: Trigger.TriggerContext): TriggerResult = TriggerResult.CONTINUE

    override def onEventTime(time: Long, window: TimeWindow, ctx: Trigger.TriggerContext): TriggerResult = TriggerResult.CONTINUE

    override def clear(window: TimeWindow, ctx: Trigger.TriggerContext): Unit = {}
  }

  // 当前数据进行处理
  class UvCountResultWithBloomFilter extends ProcessWindowFunction[(String, Long), UvCount, String, TimeWindow] {
    var jedis: Jedis = _
    var bloom: Bloom = _

    override def open(parameters: Configuration): Unit = {
      // 创建 Redis 连接
      jedis = new Jedis("nn1.hadoop", 6379)
      // 位图大小 2^30 占用128M
      bloom = new Bloom(1 << 30)
    }

    override def process(key: String, context: Context, elements: Iterable[(String, Long)], out: Collector[UvCount]): Unit = {
      // 位图使用当前窗口的 windowEnd 作为 key,保存到 redis 中
      val storedKey: String = context.window.getEnd.toString
      // 将每个窗口的 UvCount 值,作为状态存入redis中. 存成一张countMap的表
      val countMap: String = "countMap"
      // 先获取当前count值
      var count: Long = 0L
      if (jedis.hget(countMap, storedKey) != null) {
        count = jedis.hget(countMap, storedKey).toLong
      }
      //获取 userId
      val userId: String = elements.last._2.toString
      val offset: Long = bloom.hash(userId, 79)
      val isExist: lang.Boolean = jedis.getbit(storedKey, offset)

      // 如果不存在,那么就将对应位置置1,count + 1;如果存在,不做操作
      if (!isExist) {
        jedis.setbit(storedKey, offset, true)
        jedis.hset(countMap, storedKey, (count + 1).toString)
      }
    }

    override def close(): Unit = {
      jedis.close()
    }
  }

  // 自定义一个布隆过滤器
  class Bloom(size: Long) extends Serializable {
    // 定义位图的大小，应该是2的整次幂
    private val cap: Long = size

    // 实现一个hash函数
    def hash(str: String, seed: Int): Long = {
      var result: Int = 0
      for (i <- 0 until str.length) {
        result = result * seed + str.charAt(i)
      }
      // 返回一个在cap范围内的一个值
      (cap - 1) & result
    }
  }

}