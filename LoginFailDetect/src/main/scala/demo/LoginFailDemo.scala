package demo

import org.apache.flink.api.common.state.{ListState, ListStateDescriptor, ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

import scala.collection.mutable.ListBuffer

/**
 * Copyright (c) 2018-2028 尚硅谷 All Rights Reserved
 *
 * Project: UserBehaviorAnalysis
 * Package: com.atguigu.loginfail_detect
 * Version: 1.0
 *
 * Created by wushengran on 2020/4/28 14:30
 */

// 定义输入输出的样例类
case class LoginEvent(userId: Long, ip: String, eventType: String, eventTime: Long)

case class Warning(userId: Long, firstFailTime: Long, lastFailTime: Long, warningMsg: String)

object LoginFailDemo {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    // 从文件读取数据，map成样例类，并分配时间戳和watermark
    val resource = getClass.getResource("/LoginLog.csv")
    //    val loginEventStream: DataStream[LoginEvent] = env.readTextFile(resource.getPath)
    val loginEventStream: DataStream[LoginEvent] = env.readTextFile("E:\\A_data\\3.code\\UserBehaviorAnalysis\\LoginFailDetect\\src\\main\\resources\\LoginLog.csv")
      .map(data => {
        val dataArray = data.split(",")
        LoginEvent(dataArray(0).toLong, dataArray(1), dataArray(2), dataArray(3).toLong)
      })
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[LoginEvent](Time.seconds(3)) {
        override def extractTimestamp(element: LoginEvent): Long = element.eventTime * 1000L
      })

    // 用ProcessFunction进行转换，如果遇到2秒内连续2次登录失败，就输出报警
    val loginWarningStream: DataStream[Warning] = loginEventStream
      .keyBy(_.userId)
      .process(new LoginFailWarning(2))

    loginWarningStream.print()

    env.execute("login fail job")
  }
}

// 实现自定义的ProcessFunction
class LoginFailWarning(maxFailTimes: Int) extends KeyedProcessFunction[Long, LoginEvent, Warning] {
  // 定义List状态，用来保存2秒内所有的登录失败事件
  lazy val loginFailListState: ListState[LoginEvent] = getRuntimeContext.getListState(new ListStateDescriptor[LoginEvent]("saved-loginfail", classOf[LoginEvent]))
  // 定义Value状态，用来保存定时器的时间戳
  lazy val timerTsState: ValueState[Long] = getRuntimeContext.getState(new ValueStateDescriptor[Long]("timer-ts", classOf[Long]))

  override def processElement(value: LoginEvent, ctx: KeyedProcessFunction[Long, LoginEvent, Warning]#Context, out: Collector[Warning]): Unit = {
    // 判断当前数据是否是登录失败
    if (value.eventType == "fail") {
      // 如果是失败，那么添加到ListState里，如果没有注册过定时器，就注册
      loginFailListState.add(value)
      if (timerTsState.value() == 0) {
        val ts = value.eventTime * 1000L + 2000L
        ctx.timerService().registerEventTimeTimer(ts)
        timerTsState.update(ts)
      }
    } else {
      // 如果是登录成功，删除定时器，重新开始
      ctx.timerService().deleteEventTimeTimer(timerTsState.value())
      loginFailListState.clear()
      timerTsState.clear()
    }
  }

  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, LoginEvent, Warning]#OnTimerContext, out: Collector[Warning]): Unit = {
    // 如果2秒后的定时器触发了，那么判断ListState中失败的个数
    val allLoginFailList: ListBuffer[LoginEvent] = new ListBuffer[LoginEvent]
    val iter = loginFailListState.get().iterator()
    while (iter.hasNext)
      allLoginFailList += iter.next()

    if (allLoginFailList.length >= maxFailTimes) {
      out.collect(Warning(ctx.getCurrentKey,
        allLoginFailList.head.eventTime,
        allLoginFailList.last.eventTime,
        "login fail in 2s for " + allLoginFailList.length + " times.")
      )
    }
    // 清空状态
    loginFailListState.clear()
    timerTsState.clear()
  }
}