package demo

import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.triggers.{Trigger, TriggerResult}
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector
import redis.clients.jedis.Jedis

/**
  * Copyright (c) 2018-2028 尚硅谷 All Rights Reserved 
  *
  * Project: UserBehaviorAnalysis
  * Package: com.atguigu.networkflow_analysis
  * Version: 1.0
  *
  * Created by wushengran on 2020/4/27 15:45
  */
object UvWithBloomFilterDemo {
  def main(args: Array[String]): Unit = {
    // 创建一个流处理执行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    // 从文件读取数据
    val inputStream: DataStream[String] = env.readTextFile("D:\\Projects\\BigData\\UserBehaviorAnalysis\\HotItemsAnalysis\\src\\main\\resources\\UserBehavior.csv")

    // 将数据转换成样例类类型，并且提取timestamp定义watermark
    val dataStream: DataStream[UserBehavior] = inputStream
      .map( data => {
        val dataArray = data.split(",")
        UserBehavior( dataArray(0).toLong, dataArray(1).toLong, dataArray(2).toInt, dataArray(3), dataArray(4).toLong )
      } )
      .assignAscendingTimestamps(_.timestamp * 1000L)

    // 分配key，包装成二元组开创聚合
    val uvStream: DataStream[UvCount] = dataStream
      .filter(_.behavior == "pv")
      .map( data => ("uv", data.userId) )
      .keyBy(_._1)
      .timeWindow(Time.hours(1))
      .trigger(new MyTrigger())    // 自定义Trigger
      .process( new UvCountResultWithBloomFilter() )

    uvStream.print()

    env.execute("uv job")
  }
}

// 自定义一个触发器，每来一条数据就触发一次窗口计算操作
class MyTrigger() extends Trigger[(String, Long), TimeWindow]{
  override def onEventTime(time: Long, window: TimeWindow, ctx: Trigger.TriggerContext): TriggerResult = TriggerResult.CONTINUE

  override def onProcessingTime(time: Long, window: TimeWindow, ctx: Trigger.TriggerContext): TriggerResult = TriggerResult.CONTINUE

  override def clear(window: TimeWindow, ctx: Trigger.TriggerContext): Unit = {}

  // 数据来了之后，触发计算并清空状态，不保存数据
  override def onElement(element: (String, Long), timestamp: Long, window: TimeWindow, ctx: Trigger.TriggerContext): TriggerResult = TriggerResult.FIRE_AND_PURGE
}

// 自定义ProcessWindowFunction，把当前数据进行处理，位图保存在redis中
class UvCountResultWithBloomFilter() extends ProcessWindowFunction[(String, Long), UvCount, String, TimeWindow]{
  var jedis: Jedis = _
  var bloom: Bloom = _

  override def open(parameters: Configuration): Unit = {
    jedis = new Jedis("localhost", 6379)
    // 位图大小10亿个位，也就是2^30，占用128MB
    bloom = new Bloom(1<<30)
  }

  // 每来一个数据，主要是要用布隆过滤器判断redis位图中对应位置是否为1
  override def process(key: String, context: Context, elements: Iterable[(String, Long)], out: Collector[UvCount]): Unit = {
    // bitmap用当前窗口的end作为key，保存到redis里，（windowEnd，bitmap）
    val storedKey = context.window.getEnd.toString

    // 我们把每个窗口的uv count值，作为状态也存入redis中，存成一张叫做countMap的表
    val countMap = "countMap"
    // 先获取当前的count值
    var count = 0L
    if( jedis.hget(countMap, storedKey) != null )
      count = jedis.hget(countMap, storedKey).toLong

    // 取userId，计算hash值，判断是否在位图中
    val userId = elements.last._2.toString
    val offset = bloom.hash(userId, 61)
    val isExist = jedis.getbit( storedKey, offset )

    // 如果不存在，那么就将对应位置置1，count加1；如果存在，不做操作
    if( !isExist ){
      jedis.setbit( storedKey, offset, true )
      jedis.hset( countMap, storedKey, (count + 1).toString )
    }
  }
}

// 自定义一个布隆过滤器
class Bloom(size: Long) extends Serializable{
  // 定义位图的大小，应该是2的整次幂
  private val cap = size

  // 实现一个hash函数
  def hash(str: String, seed: Int): Long = {
    var result = 0
    for( i <- 0 until str.length ){
      result = result * seed + str.charAt(i)
    }
    // 返回一个在cap范围内的一个值
    (cap - 1) & result
  }
}