import java.util

import LoginFail.{LoginEvent, Warning}
import org.apache.flink.cep.PatternSelectFunction
import org.apache.flink.cep.scala.{CEP, PatternStream}
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.configuration.{Configuration, RestOptions}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

/**
 * @Auther: wxf
 * @Date: 2022/11/22 09:54:57
 * @Description: LoginFailWithCep
 * @Version 1.0.0
 */
object LoginFailWithCep {
  def main(args: Array[String]): Unit = {

    val conf: Configuration = new Configuration()
    conf.setInteger(RestOptions.PORT, 8081)
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf)
    env.setParallelism(1)
    // 设置时间语义 事件时间语义
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val inputPath: String = LoginFail.getClass.getResource("LoginLogCEP.csv").getPath
    val inputStream: DataStream[String] = env.readTextFile(inputPath).uid("input")

    val mapStream: DataStream[LoginEvent] = inputStream.map(x => {
      val str: Array[String] = x.split(",")
      LoginEvent(str(0).toLong, str(1), str(2), str(3).toLong * 1000L)
    }).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[LoginEvent](Time.seconds(3)) {
      override def extractTimestamp(element: LoginEvent): Long = element.timeStamp
    })

    // 分组 keyBy
    val keyByStream: KeyedStream[LoginEvent, Long] = mapStream.keyBy(_.userId)

    //    // 定义 CEP Pattern
    //    val loginFailPattern: Pattern[LoginEvent, LoginEvent] = Pattern
    //      .begin[LoginEvent]("firstFail").where(_.eventTpye == "fail") // 第一次登录失败
    //      .next("secondFail").where(_.eventTpye == "fail") // 第二次登录失败
    //      //      .next("secondFail").where(_.eventTpye == "fail").times(2) // 连续两次登录失败
    //      .within(Time.seconds(5)) // 2秒内 进行判断
    //
    //    val patternStream: PatternStream[LoginEvent] = CEP.pattern(keyByStream, loginFailPattern)
    //
    //    val resultStream: DataStream[Warning] = patternStream.select(new PatternSelectFunction[LoginEvent, Warning] {
    //      override def select(pattern: util.Map[String, util.List[LoginEvent]]) = {
    //        val firstFail: LoginEvent = pattern.get("firstFail").get(0)
    //        val secondFail: LoginEvent = pattern.get("secondFail").get(0)
    //        Warning(firstFail.userId, firstFail.timeStamp, secondFail.timeStamp, "login fail!")
    //      }
    //    })

    // 定义 CEP Pattern  consecutive 表示严格紧邻 循环默认是宽松紧邻
    val loginFailPattern: Pattern[LoginEvent, LoginEvent] = Pattern
      .begin[LoginEvent]("firstFail").where(_.eventTpye == "fail").times(3).consecutive()
      .within(Time.seconds(5)) // 2秒内 进行判断

    val patternStream: PatternStream[LoginEvent] = CEP.pattern(keyByStream, loginFailPattern)

    val resultStream: DataStream[Warning] = patternStream.select(new PatternSelectFunction[LoginEvent, Warning] {
      override def select(pattern: util.Map[String, util.List[LoginEvent]]) = {
        val eventList: util.List[LoginEvent] = pattern.get("firstFail")
        val firstFail: LoginEvent = eventList.get(0)
        val lastFail: LoginEvent = eventList.get(eventList.size() - 1)
        Warning(firstFail.userId, firstFail.timeStamp, lastFail.timeStamp, "login fail!")
      }
    })


    resultStream.print("resultStream：")
    env.execute("LoginFailWithCep")
  }
}