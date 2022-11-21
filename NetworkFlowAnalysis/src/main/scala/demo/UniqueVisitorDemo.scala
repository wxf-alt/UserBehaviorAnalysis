package demo

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.AllWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

/**
  * Copyright (c) 2018-2028 尚硅谷 All Rights Reserved 
  *
  * Project: UserBehaviorAnalysis
  * Package: com.atguigu.networkflow_analysis
  * Version: 1.0
  *
  * Created by wushengran on 2020/4/27 14:39
  */

case class UvCount( windowEnd: Long, count: Long )

object UniqueVisitorDemo {
  def main(args: Array[String]): Unit = {
    // 创建一个流处理执行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    // 从文件读取数据
    val inputStream: DataStream[String] = env.readTextFile("E:\\A_data\\3.code\\UserBehaviorAnalysis\\HotItemsAnalysis\\src\\main\\resources\\UserBehavior.csv")

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
      .timeWindowAll(Time.hours(1))    // 基于DataStream开一小时的滚动窗口进行统计
//      .apply( new UvCountResult() )
      .aggregate( new UvCountAgg(), new UvCountResultWithIncreAgg() )

    uvStream.print()

    env.execute("uv job")
  }
}

// 自定义全窗口函数
class UvCountResult() extends AllWindowFunction[UserBehavior, UvCount, TimeWindow]{
  override def apply(window: TimeWindow, input: Iterable[UserBehavior], out: Collector[UvCount]): Unit = {
      // 定义一个Set类型来保存所有的userId，自动去重
      var idSet = Set[Long]()
      // 将当前窗口的所有数据，添加到set里
      for( userBehavior <- input ){
        idSet += userBehavior.userId
      }
      // 输出set的大小，就是去重之后的UV值
      out.collect( UvCount(window.getEnd, idSet.size) )
  }
}

// 自定义增量聚合函数，需要定义一个Set作为累加状态
class UvCountAgg() extends AggregateFunction[UserBehavior, Set[Long], Long]{
  override def add(value: UserBehavior, accumulator: Set[Long]): Set[Long] = accumulator + value.userId

  override def createAccumulator(): Set[Long] = Set[Long]()

  override def getResult(accumulator: Set[Long]): Long = accumulator.size

  override def merge(a: Set[Long], b: Set[Long]): Set[Long] = a ++ b
}
// 自定义窗口函数，添加window信息包装成样例类
class UvCountResultWithIncreAgg() extends AllWindowFunction[Long, UvCount, TimeWindow]{
  override def apply(window: TimeWindow, input: Iterable[Long], out: Collector[UvCount]): Unit = {
    out.collect( UvCount(window.getEnd, input.head) )
  }
}