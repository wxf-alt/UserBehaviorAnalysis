package demo

import org.apache.flink.api.common.functions.{AggregateFunction, MapFunction, RichMapFunction}
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import scala.util.Random

/**
  * Copyright (c) 2018-2028 尚硅谷 All Rights Reserved 
  *
  * Project: UserBehaviorAnalysis
  * Package: com.atguigu.networkflow_analysis
  * Version: 1.0
  *
  * Created by wushengran on 2020/4/27 11:47
  */

// 定义输入输出的样例类
case class UserBehavior( userId: Long, itemId: Long, categoryId: Int, behavior: String, timestamp: Long )
case class PvCount( windowEnd: Long, count: Long )

object PageViewDemo {
  def main(args: Array[String]): Unit = {
    // 创建一个流处理执行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(4)
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
    val pvStream: DataStream[PvCount] = dataStream
      .filter(_.behavior == "pv")
//      .map( data => ("pv", 1L) )    // map成二元组("pv", count)
      .map( new MyMapper() )    // 自定义Mapper，将key均匀分配
      .keyBy(_._1)    // 把所有数据分到一组做总计
      .timeWindow(Time.hours(1))    // 开一小时的滚动窗口进行统计
      .aggregate( new PvCountAgg(), new PvCountResult() )

    // 把各分区的结果汇总起来
    val pvTotalStream: DataStream[PvCount] = pvStream
      .keyBy(_.windowEnd)
      .process( new TotalPvCountResult() )
//      .sum("count")

    pvTotalStream.print()

    env.execute("pv job")
  }
}

// 自定义预聚合函数
class PvCountAgg() extends AggregateFunction[(String, Long), Long, Long]{
  override def add(value: (String, Long), accumulator: Long): Long = accumulator + 1

  override def createAccumulator(): Long = 0L

  override def getResult(accumulator: Long): Long = accumulator

  override def merge(a: Long, b: Long): Long = a + b
}

// 自定义窗口函数，把窗口信息包装到样例类类型输出
class PvCountResult() extends WindowFunction[Long, PvCount, String, TimeWindow]{
  override def apply(key: String, window: TimeWindow, input: Iterable[Long], out: Collector[PvCount]): Unit = {
    out.collect( PvCount(window.getEnd, input.head) )
  }
}

// 自定义MapFunction，随机生成key
class MyMapper() extends RichMapFunction[UserBehavior, (String, Long)]{
  lazy val index: Long = getRuntimeContext.getIndexOfThisSubtask
  override def map(value: UserBehavior): (String, Long) = (index.toString, 1L)
}

// 自定义ProcessFunction，将聚合结果按窗口合并
class TotalPvCountResult() extends KeyedProcessFunction[Long, PvCount, PvCount]{
  // 定义一个状态，用来保存当前所有结果之和
  lazy val totalCountState: ValueState[Long] = getRuntimeContext.getState(new ValueStateDescriptor[Long]("total-count", classOf[Long]))

  override def processElement(value: PvCount, ctx: KeyedProcessFunction[Long, PvCount, PvCount]#Context, out: Collector[PvCount]): Unit = {
    // 加上新的count值，更新状态
    totalCountState.update( totalCountState.value() + value.count )
    // 注册定时器，windowEnd+1之后触发
    ctx.timerService().registerEventTimeTimer(value.windowEnd + 1)
  }

  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, PvCount, PvCount]#OnTimerContext, out: Collector[PvCount]): Unit = {
    // 定时器触发时，所有分区count值都已到达，输出总和
    out.collect( PvCount(ctx.getCurrentKey, totalCountState.value()) )
    totalCountState.clear()
  }
}