package Demo

import java.sql.Timestamp

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.{EnvironmentSettings, Slide}
import org.apache.flink.table.api.scala._

/**
  * Copyright (c) 2018-2028 尚硅谷 All Rights Reserved 
  *
  * Project: UserBehaviorAnalysis
  * Package: com.atguigu.hotitems_analysis
  * Version: 1.0
  *
  * Created by wushengran on 2020/4/29 17:27
  */
object HotItemsWithSQLDemo {
  def main(args: Array[String]): Unit = {
    // 创建一个流处理执行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val inputStream: DataStream[String] = env.readTextFile("D:\\Projects\\BigData\\UserBehaviorAnalysis\\HotItemsAnalysis\\src\\main\\resources\\UserBehavior.csv")
    // 将数据转换成样例类类型，并且提取timestamp定义watermark
    val dataStream: DataStream[UserBehavior] = inputStream
      .map( data => {
        val dataArray = data.split(",")
        UserBehavior( dataArray(0).toLong, dataArray(1).toLong, dataArray(2).toInt, dataArray(3), dataArray(4).toLong )
      } )
      .assignAscendingTimestamps(_.timestamp * 1000L)

    // 要调用Table API，先创建表执行环境
    val settings = EnvironmentSettings.newInstance()
      .useBlinkPlanner()
      .inStreamingMode()
      .build()
    val tableEnv = StreamTableEnvironment.create(env, settings)

    // 将DataStream注册成表，提取需要的字段，进行处理
    tableEnv.createTemporaryView("data_table", dataStream, 'itemId, 'behavior, 'timestamp.rowtime as 'ts)

    // 用SQL实现
    val resultTable = tableEnv.sqlQuery(
      """
        |select *
        |from (
        |    select *,
        |      row_number() over (partition by windowEnd order by cnt desc) as row_num
        |    from (
        |      select itemId,
        |          count(itemId) as cnt,
        |          hop_end(ts, interval '5' minute, interval '1' hour) as windowEnd
        |      from data_table
        |      where behavior = 'pv'
        |      group by hop(ts, interval '5' minute, interval '1' hour), itemId
        |    )
        |)
        |where row_num <= 5
      """.stripMargin)

    resultTable.toRetractStream[(Long, Long, Timestamp, Long)].print("result")

    env.execute("hot item with sql")
  }
}
