package com.abigtomato.spark.scala.streaming.streamingOnKafka

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.{Seconds, StreamingContext}

object StreamingTest {
  def main(args: Array[String]): Unit = {
    //创建sparkConf
    val sparkConf = new SparkConf().setAppName("KafkaOffsetDemo").setMaster("local")
    // 创建StreamingContext batch size 为 5秒
    val ssc = new StreamingContext(sparkConf, Seconds(5))
    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "node1:9092,node2:9092,node3:9092", //kafka集群地址
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "KafkaOffsetDemo", //消费者组名
      "auto.offset.reset" -> "earliest", //当各分区下有已提交的offset时，从提交的offset开始消费；无提交的offset时，从头开始消费
      "enable.auto.commit" -> (false: java.lang.Boolean)) //如果是true，则这个消费者的偏移量会在后台自动提交
    val topics = Array("topic1")
    val value: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream[String, String](
      ssc,
      PreferConsistent,
      Subscribe[String, String](topics, kafkaParams))
    //消费主题
    //创建DStream，返回接收到的输入数据
    val stream = value
    // 打印获取到的数据，因为5秒刷新一次，所以数据长度大于0时才打印
    stream.foreachRDD(rdd => {

      rdd.foreach((info:ConsumerRecord[String, String]) => {
        val value = info.value()
        info.offset()
        println(value)

      })




//      if (rdd.count > 0) {
//        println("=============================")
//        println("打印获取到的kafka里的内容")
//        rdd.foreach(info => {
//          val value = info.value()
//          println(value)
//
//        })
//        println("=============================")
//        println("打印offset的信息")
//        // offset
//        val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
//
//        //打印offset
//        rdd.foreachPartition { iter =>
//          val o: OffsetRange = offsetRanges(TaskContext.get.partitionId)
//          println(s"${o.topic} ${o.partition} ${o.fromOffset} ${o.untilOffset}")
//        }
//        println("=============================")
//        // 等输出操作完成后提交offset
//        stream.asInstanceOf[CanCommitOffsets].commitAsync(offsetRanges)
//
//      }
    })
    //启动
    ssc.start()
    //等待停止
    ssc.awaitTermination()
  }
}
