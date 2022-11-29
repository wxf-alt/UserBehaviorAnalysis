package demo

import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.co.CoProcessFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

/**
  * Copyright (c) 2018-2028 尚硅谷 All Rights Reserved 
  *
  * Project: UserBehaviorAnalysis
  * Package: com.atguigu.orderpay_detect
  * Version: 1.0
  *
  * Created by wushengran on 2020/4/29 14:29
  */

// 定义到账数据的样例类
case class ReceiptEvent( txId: String, payChannel: String, timestamp: Long )

object OrderPayTxMatch {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)

    // 从文件中读取数据，并转换成样例类
    val resource1 = getClass.getResource("/OrderLog.csv")
    val orderEventStream: DataStream[OrderEvent] = env.readTextFile(resource1.getPath)
      //      val orderEventStream = env.socketTextStream("localhost", 7777)
      .map( data => {
      val dataArray = data.split(",")
      OrderEvent( dataArray(0).toLong, dataArray(1), dataArray(2), dataArray(3).toLong )
    } )
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[OrderEvent](Time.seconds(3)) {
        override def extractTimestamp(element: OrderEvent): Long = element.eventTime * 1000L
      })
      .filter(_.txId != "")     // 只过滤出pay事件
      .keyBy(_.txId)

    val resource2 = getClass.getResource("/ReceiptLog.csv")
    val receiptEventStream: DataStream[ReceiptEvent] = env.readTextFile(resource2.getPath)
      //      val orderEventStream = env.socketTextStream("localhost", 7777)
      .map( data => {
      val dataArray = data.split(",")
      ReceiptEvent( dataArray(0), dataArray(1), dataArray(2).toLong )
    } )
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[ReceiptEvent](Time.seconds(3)) {
        override def extractTimestamp(element: ReceiptEvent): Long = element.timestamp * 1000L
      })
      .keyBy(_.txId)

    // 用connect连接两条流，匹配事件进行处理
    val resultStream: DataStream[(OrderEvent, ReceiptEvent)] =  orderEventStream
      .connect(receiptEventStream)
      .process( new OrderPayTxDetect() )

    val unmatchedPays = new OutputTag[OrderEvent]("unmatched-pays")
    val unmatchedReceipts = new OutputTag[ReceiptEvent]("unmatched-receipts")

    resultStream.print("matched")
    resultStream.getSideOutput(unmatchedPays).print("unmatched-pays")
    resultStream.getSideOutput(unmatchedReceipts).print("unmatched-receipts")
    env.execute("order pay tx match job")
  }
}

// 自定义CoProcessFunction，实现两条流数据的匹配检验
class OrderPayTxDetect() extends CoProcessFunction[OrderEvent, ReceiptEvent, (OrderEvent, ReceiptEvent)]{
  // 用两个ValueState，保存当前交易对应的支付事件和到账事件
  lazy val payState: ValueState[OrderEvent] = getRuntimeContext.getState(new ValueStateDescriptor[OrderEvent]("pay", classOf[OrderEvent]))
  lazy val receiptState: ValueState[ReceiptEvent] = getRuntimeContext.getState(new ValueStateDescriptor[ReceiptEvent]("receipt", classOf[ReceiptEvent]))

  val unmatchedPays = new OutputTag[OrderEvent]("unmatched-pays")
  val unmatchedReceipts = new OutputTag[ReceiptEvent]("unmatched-receipts")

  override def processElement1(pay: OrderEvent, ctx: CoProcessFunction[OrderEvent, ReceiptEvent, (OrderEvent, ReceiptEvent)]#Context, out: Collector[(OrderEvent, ReceiptEvent)]): Unit = {
    // pay来了，考察有没有对应的receipt来过
    val receipt = receiptState.value()
    if( receipt != null ){
      // 如果已经有receipt，那么正常匹配，输出到主流
      out.collect( (pay, receipt) )
      receiptState.clear()
    } else{
      // 如果receipt还没来，那么把pay存入状态，注册一个定时器等待5秒
      payState.update(pay)
      ctx.timerService().registerEventTimeTimer( pay.eventTime * 1000L + 5000L )
    }
  }

  override def processElement2(receipt: ReceiptEvent, ctx: CoProcessFunction[OrderEvent, ReceiptEvent, (OrderEvent, ReceiptEvent)]#Context, out: Collector[(OrderEvent, ReceiptEvent)]): Unit = {
    // receipt来了，考察有没有对应的pay来过
    val pay = payState.value()
    if( pay != null ){
        // 如果已经有pay，那么正常匹配，输出到主流
        out.collect( (pay, receipt) )
        payState.clear()
    } else{
      // 如果pay还没来，那么把receipt存入状态，注册一个定时器等待3秒
      receiptState.update(receipt)
      ctx.timerService().registerEventTimeTimer( receipt.timestamp * 1000L + 3000L )
    }
  }

  // 定时器触发，有两种情况，所以要判断当前有没有pay和receipt
  override def onTimer(timestamp: Long, ctx: CoProcessFunction[OrderEvent, ReceiptEvent, (OrderEvent, ReceiptEvent)]#OnTimerContext, out: Collector[(OrderEvent, ReceiptEvent)]): Unit = {
    // 如果pay不为空，说明receipt没来，输出unmatchedPays
    if( payState.value() != null )
      ctx.output(unmatchedPays, payState.value())
    if( receiptState.value() != null )
      ctx.output(unmatchedReceipts, receiptState.value())
    // 清空状态
    payState.clear()
    receiptState.clear()
  }
}