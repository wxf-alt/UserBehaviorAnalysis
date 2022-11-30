import HotItems.UserBehavior
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.table.api.{EnvironmentSettings, Table}
import org.apache.flink.table.api.scala._
import org.apache.flink.types.Row

/**
 * @Auther: wxf
 * @Date: 2022/11/30 13:03:00
 * @Description: HotItemsWithSQL
 * @Version 1.0.0
 */
object HotItemsWithSQL {
  def main(args: Array[String]): Unit = {

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration())
    env.setParallelism(1)
    // 设置时间语义 事件时间语义
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val inputPath: String = HotItems.getClass.getResource("UserBehavior.csv").getPath
    val inputStream: DataStream[String] = env.readTextFile(inputPath)

    val mapStream: DataStream[UserBehavior] = inputStream.map(x => {
      val str: Array[String] = x.split(",")
      UserBehavior(str(0).toLong, str(1).toLong, str(2).toInt, str(3), str(4).toLong)
    })
      // 设置 waterMark 和 定义时间戳
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[UserBehavior](Time.seconds(1)) {
        override def extractTimestamp(element: UserBehavior): Long = element.timeStamp * 1000L
      })

    val settings: EnvironmentSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build()
    val tableEnv: StreamTableEnvironment = StreamTableEnvironment.create(env, settings)

    // 将数据流 注册 成表
    tableEnv.createTemporaryView("data_table",mapStream,'itemId, 'behavior, 'timeStamp.rowtime as 'ts)

    val resultTable: Table = tableEnv.sqlQuery(
      """select * from
        |(
        | select itemId, cnt, windowEnd, row_number() over(partition by windowEnd order by cnt desc) as rn
        | from
        |   (
        |   select itemId,
        |     count(itemId) as cnt,
        |     hop_end(ts, interval '5' minute, interval '1' hour) as windowEnd
        |   from data_table where behavior = 'pv'
        |   group by itemId, hop(ts, interval '5' minute, interval '1' hour)
        |   )
        |) where rn <= 5
        |""".stripMargin)

    resultTable.toRetractStream[Row].print("resultTable:")

    env.execute("HotItemsWithSQL")
  }
}