import java.util

import org.apache.flink.api.common.state.{ListState, ListStateDescriptor, ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.{Configuration, RestOptions}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

/**
 * @Auther: wxf
 * @Date: 2022/11/22 09:54:45
 * @Description: LoginFail  登录失败 统计  2秒内连续2次登录失败的用户 输出报警
 * @Version 1.0.0
 */
object LoginFail {
  def main(args: Array[String]): Unit = {
    val conf: Configuration = new Configuration()
    conf.setInteger(RestOptions.PORT, 8081)
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf)
    env.setParallelism(1)
    // 设置时间语义 事件时间语义
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val inputPath: String = LoginFail.getClass.getResource("LoginLog.csv").getPath
    val inputStream: DataStream[String] = env.readTextFile(inputPath).uid("input").name("name-input")

    val mapStream: DataStream[LoginEvent] = inputStream.map(x => {
      val str: Array[String] = x.split(",")
      Thread.sleep(1000)
      LoginEvent(str(0).toLong, str(1), str(2), str(3).toLong * 1000L)
    }).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[LoginEvent](Time.seconds(3)) {
      override def extractTimestamp(element: LoginEvent): Long = element.timeStamp
    }).uid("map").name("name-map")

    mapStream.print("mapStream：")

    val resultStream: DataStream[Warning] = mapStream
      .keyBy(_.userId)
      .process(new LoginFailWarning(2)) // 连续2秒内2次登录失败，输出报警

    resultStream.print("resultStream：")
    env.execute("LoginFail")
  }

  class LoginFailWarning(maxFailTimes: Int) extends KeyedProcessFunction[Long, LoginEvent, Warning] {
    // 定义 List 状态，保存2秒内所有的登录失败事件
    lazy val loginFailListState: ListState[LoginEvent] = getRuntimeContext.getListState(new ListStateDescriptor[LoginEvent]("saved-loginFail", classOf[LoginEvent]))
    // 定义 valueState，用来保存定时器时间戳
    lazy val timerTsState: ValueState[Long] = getRuntimeContext.getState(new ValueStateDescriptor[Long]("timer-ts", classOf[Long]))

    override def processElement(value: LoginEvent, ctx: KeyedProcessFunction[Long, LoginEvent, Warning]#Context, out: Collector[Warning]): Unit = {
      // 判断当前数据是否是登录失败
      if (value.eventTpye == "fail") { // 失败数据 添加到失败状态中。没有注册定时器 就注册
        loginFailListState.add(value)
        if (timerTsState.value() == 0) { // 没有定时器 注册定时器
          val ts: Long = value.timeStamp + 2000L
          ctx.timerService().registerEventTimeTimer(ts)
          timerTsState.update(ts)
        }
      } else { // 如果登录成功，删除定时器 重新开始
        ctx.timerService().deleteEventTimeTimer(timerTsState.value())
        loginFailListState.clear()
        timerTsState.clear()
      }
    }

    override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, LoginEvent, Warning]#OnTimerContext, out: Collector[Warning]): Unit = {
      // 如果 2秒后的定时器 触发，需要判断 List状态中是否超过次数(maxFailTimes)
      val loginFailEvents: util.Iterator[LoginEvent] = loginFailListState.get().iterator()
      import scala.collection.convert.wrapAll._
      val list: List[LoginEvent] = loginFailEvents.toList
      if (list.size >= maxFailTimes) {
        val userId: Long = ctx.getCurrentKey
        val firstTime: Long = list.head.timeStamp
        val lastTime: Long = list.last.timeStamp
        val msg: String = s"login fail in ${maxFailTimes}s for ${list.size} times."
        out.collect(Warning(userId, firstTime, lastTime, msg))
      }
      // 没有达到报警标准 清空状态
      loginFailListState.clear()
      timerTsState.clear()
    }

  }

  // 输入输出样例类
  // 5402,83.149.11.115,success,1558430815
  case class LoginEvent(userId: Long, ip: String, eventTpye: String, timeStamp: Long)

  case class Warning(userId: Long, firstFailTime: Long, lastFailTime: Long, warningMsg: String)

}