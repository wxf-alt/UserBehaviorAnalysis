package demo

import java.sql.Timestamp

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

/**
  * Copyright (c) 2018-2028 尚硅谷 All Rights Reserved 
  *
  * Project: UserBehaviorAnalysis
  * Package: com.atguigu.market_analysis
  * Version: 1.0
  *
  * Created by wushengran on 2020/4/28 10:41
  */

// 定义输入输出样例类
case class AdClickEvent( userId: Long, adId: Long, province: String, city: String, timestamp: Long )
case class AdCountByProvince( province: String, windowEnd: String, count: Long)
// 定义侧输出流报警信息样例类
case class BlackListWarning( userId: Long, adId: Long, msg: String)

object AdAnalysisByProvinceDemo {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)

    // 从文件读取数据，转换成样例类，并提取时间戳生成watermark
    val resource = getClass.getResource("/AdClickLog.csv")
    val adLogStream: DataStream[AdClickEvent] = env.readTextFile(resource.getPath)
      .map( data => {
        val dataArray = data.split(",")
        AdClickEvent( dataArray(0).toLong, dataArray(1).toLong, dataArray(2), dataArray(3), dataArray(4).toLong )
      } )
      .assignAscendingTimestamps(_.timestamp * 1000L)

    // 定义刷单行为过滤操作
    val filterBlackListStream: DataStream[AdClickEvent] = adLogStream
      .keyBy( data => (data.userId, data.adId) )    // 按照用户和广告id分组
      .process( new FilterBlackList(100L) )

    // 按照province分组开窗聚合统计
    val adCountStream: DataStream[AdCountByProvince] = filterBlackListStream
      .keyBy(_.province)
      .timeWindow( Time.hours(1), Time.seconds(5) )
      .aggregate( new AdCountAgg(), new AdCountResult() )

    adCountStream.print()
    filterBlackListStream.getSideOutput(new OutputTag[BlackListWarning]("blacklist")).print("blacklist")
    env.execute("ad analysis job")
  }
}

// 自定义预聚合函数
class AdCountAgg() extends AggregateFunction[AdClickEvent, Long, Long]{
  override def add(value: AdClickEvent, accumulator: Long): Long = accumulator + 1

  override def createAccumulator(): Long = 0L

  override def getResult(accumulator: Long): Long = accumulator

  override def merge(a: Long, b: Long): Long = a + b
}

class AdCountResult() extends WindowFunction[Long, AdCountByProvince, String, TimeWindow]{
  override def apply(key: String, window: TimeWindow, input: Iterable[Long], out: Collector[AdCountByProvince]): Unit = {
    val province = key
    val windowEnd = new Timestamp(window.getEnd).toString
    val count = input.iterator.next()
    out.collect( AdCountByProvince(province, windowEnd, count) )
  }
}

// 实现自定义的ProcessFunction，判断用户对广告的点击次数是否达到上限
class FilterBlackList(maxClickCount: Long) extends KeyedProcessFunction[(Long, Long), AdClickEvent, AdClickEvent]{
  // 定义状态，需要保存当前用户对当前广告的点击量count
  lazy val countState: ValueState[Long] = getRuntimeContext.getState(new ValueStateDescriptor[Long]("count", classOf[Long]))
  // 标识位，用来表示用户是否已经在黑名单中
  lazy val isSentState: ValueState[Boolean] = getRuntimeContext.getState(new ValueStateDescriptor[Boolean]("is-sent", classOf[Boolean]))

  override def processElement(value: AdClickEvent, ctx: KeyedProcessFunction[(Long, Long), AdClickEvent, AdClickEvent]#Context, out: Collector[AdClickEvent]): Unit = {
    // 取出状态数据
    val curCount = countState.value()

    // 如果是第一个数据，那么注册第二天0点的定时器，用于清空状态
    if( curCount == 0 ){
      val ts = (ctx.timerService().currentProcessingTime() / (1000*60*60*24) + 1) * (1000*60*60*24)
      ctx.timerService().registerProcessingTimeTimer(ts)
    }

    // 判断count值是否达到上限，如果达到，并且之前没有输出过报警信息，那么报警
    if( curCount >= maxClickCount ){
      if( !isSentState.value() ){
        ctx.output(new OutputTag[BlackListWarning]("blacklist"), BlackListWarning(value.userId, value.adId, "click over " + maxClickCount + " times today"))
        isSentState.update(true)
      }
      return
    }
    // count值加1
    countState.update(curCount + 1)
    out.collect(value)
  }

  // 0点触发定时器，直接清空状态
  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[(Long, Long), AdClickEvent, AdClickEvent]#OnTimerContext, out: Collector[AdClickEvent]): Unit = {
    countState.clear()
    isSentState.clear()
  }
}