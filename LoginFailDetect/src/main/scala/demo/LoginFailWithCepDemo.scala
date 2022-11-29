package demo

import java.util

import org.apache.flink.cep.PatternSelectFunction
import org.apache.flink.cep.scala.{CEP, PatternStream}
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

/**
  * Copyright (c) 2018-2028 尚硅谷 All Rights Reserved 
  *
  * Project: UserBehaviorAnalysis
  * Package: com.atguigu.loginfail_detect
  * Version: 1.0
  *
  * Created by wushengran on 2020/4/28 16:32
  */
object LoginFailWithCepDemo {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    // 0. 从文件读取数据，map成样例类，并分配时间戳和watermark
    val resource = getClass.getResource("/LoginLogCEP.csv")
    val loginEventStream: DataStream[LoginEvent] = env.readTextFile(resource.getPath)
      .map( data => {
        val dataArray = data.split(",")
        LoginEvent( dataArray(0).toLong, dataArray(1), dataArray(2), dataArray(3).toLong )
      } )
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[LoginEvent](Time.seconds(3)) {
        override def extractTimestamp(element: LoginEvent): Long = element.eventTime * 1000L
      })

    // 1. 定义匹配的模式
    val loginFailPattern: Pattern[LoginEvent, LoginEvent] = Pattern
      .begin[LoginEvent]("firstFail").where(_.eventType == "fail")   // 第一次登录失败
      .next("secondFail").where(_.eventType == "fail")   // 第二次登录失败
      .within(Time.seconds(2))    // 在2秒之内检测匹配

    // 2. 在分组之后的数据流上应用模式，得到一个PatternStream
    val patternStream: PatternStream[LoginEvent] = CEP.pattern(loginEventStream.keyBy(_.userId), loginFailPattern)

    // 3. 将检测到的事件序列，转换输出报警信息
    val loginFailStream: DataStream[Warning] = patternStream.select( new LoginFailDetect() )

    // 4. 打印输出
    loginFailStream.print()

    env.execute("login fail with cep job")
  }
}

// 自定义PatternSelectFunction，用来将检测到的连续登录失败事件，包装成报警信息输出
class LoginFailDetect() extends PatternSelectFunction[LoginEvent, Warning]{
  override def select(map: util.Map[String, util.List[LoginEvent]]): Warning = {
    // map里存放的就是匹配到的一组事件，key是定义好的事件模式名称
    val firstLoginFail = map.get("firstFail").get(0)
    val lastLoginFail = map.get("secondFail").get(0)
//    val lastLoginFail = map.get("firstFail").get(2)
    Warning( firstLoginFail.userId, firstLoginFail.eventTime, lastLoginFail.eventTime, "login fail!" )
  }
}
