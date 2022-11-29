package demo

import java.util

import org.apache.flink.cep.{PatternSelectFunction, PatternTimeoutFunction}
import org.apache.flink.cep.scala.CEP
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

/**
  * Copyright (c) 2018-2028 尚硅谷 All Rights Reserved
  *
  * Project: UserBehaviorAnalysis
  * Package: com.atguigu.orderpay_detect
  * Version: 1.0
  *
  * Created by wushengran on 2020/4/29 9:34
  */

// 定义输入输出数据的样例类
case class OrderEvent( orderId: Long, eventType: String, txId: String, eventTime: Long )
case class OrderResult( orderId: Long, resultMsg: String )

object OrderTimeout {
  def main(args: Array[String]): Unit = {
      val env = StreamExecutionEnvironment.getExecutionEnvironment
      env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
      env.setParallelism(1)

      // 从文件中读取数据，并转换成样例类
      val resource = getClass.getResource("/OrderLog.csv")
      val orderEventStream: DataStream[OrderEvent] = env.readTextFile(resource.getPath)
//      val orderEventStream = env.socketTextStream("localhost", 7777)
        .map( data => {
          val dataArray = data.split(",")
          OrderEvent( dataArray(0).toLong, dataArray(1), dataArray(2), dataArray(3).toLong )
        } )
        .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[OrderEvent](Time.seconds(3)) {
          override def extractTimestamp(element: OrderEvent): Long = element.eventTime * 1000L
        })

      // 1. 定义一个要匹配事件序列的模式
        val orderPayPattern = Pattern
      .begin[OrderEvent]("create").where(_.eventType == "create")    // 首先是订单的create事件
      .followedBy("pay").where(_.eventType == "pay")    // 后面来的是订单的pay事件
      .within(Time.minutes(15))

      // 2. 将pattern应用在按照orderId分组的数据流上
      val patternStream = CEP.pattern(orderEventStream.keyBy(_.orderId), orderPayPattern)

      // 3. 定义一个侧输出流标签，用来标明超时事件的侧输出流
      val orderTimeoutOutputTag = new OutputTag[OrderResult]("order timeout")

      // 4. 调用select方法，提取匹配事件和超时事件，分别进行处理转换输出
      val resultStream: DataStream[OrderResult] = patternStream
        .select( orderTimeoutOutputTag, new OrderTimeoutSelect(), new OrderPaySelect() )

      // 5. 打印输出
      resultStream.print("payed")
      resultStream.getSideOutput(orderTimeoutOutputTag).print("timeout")

      env.execute("order timeout detect job")
    }
}

// 自定义超时处理函数
class OrderTimeoutSelect() extends PatternTimeoutFunction[OrderEvent, OrderResult]{
  override def timeout(map: util.Map[String, util.List[OrderEvent]], timeoutTimestamp: Long): OrderResult = {
    val timeoutOrderId = map.get("create").iterator().next().orderId
    OrderResult( timeoutOrderId, "timeout at " + timeoutTimestamp )
  }
}

// 自定义匹配处理函数
class OrderPaySelect() extends PatternSelectFunction[OrderEvent, OrderResult]{
  override def select(map: util.Map[String, util.List[OrderEvent]]): OrderResult = {
    val payedOrderId = map.get("pay").get(0).orderId
    OrderResult( payedOrderId, "payed successfully" )
  }
}