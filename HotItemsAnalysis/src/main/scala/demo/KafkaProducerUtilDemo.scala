package demo

import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

/**
  * Copyright (c) 2018-2028 尚硅谷 All Rights Reserved
  *
  * Project: UserBehaviorAnalysis
  * Package: com.atguigu.hotitems_analysis
  * Version: 1.0
  *
  * Created by wushengran on 2020/4/25 17:37
  */
object KafkaProducerUtilDemo {
  def main(args: Array[String]): Unit = {
    writeToKakfaWithTopic("hotitems")
  }
  def writeToKakfaWithTopic(topic: String): Unit ={
    val properties = new Properties()
    properties.setProperty("bootstrap.servers", "hadoop104:9092")
    properties.setProperty("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    properties.setProperty("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

    // 创建一个KafkaProducer，用它来发送数据
    val producer = new KafkaProducer[String, String](properties)

    // 从文件中读取数据，逐条发送
    val bufferedSource = io.Source.fromFile("E:\\A_data\\3.code\\UserBehaviorAnalysis\\HotItemsAnalysis\\src\\main\\resources\\UserBehavior.csv")
    for( line <- bufferedSource.getLines() ){
      val record = new ProducerRecord[String, String](topic, line)
      producer.send(record)
    }

    producer.close()
  }
}
