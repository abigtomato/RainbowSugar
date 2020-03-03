package com.abigtomato.spark.scala.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object DStreamWordCount {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setMaster("local")
      .setAppName("DStreamWordCount")
    val streamingContext = new StreamingContext(conf, Seconds(3))

    // 从指定端口采集数据
    val socketLineDStream: ReceiverInputDStream[String] = streamingContext
      .socketTextStream("127.0.0.1", 9999)

    val wordDStream: DStream[String] = socketLineDStream.flatMap(line => line.split(","))

    val mapDStream: DStream[(String, Int)] = wordDStream.map(elem => (elem, 1))

    val wordToSumDStream: DStream[(String, Int)] = mapDStream.reduceByKey((v1, v2) => v1 + v2)

    wordToSumDStream.print()

    // 启动采集器
    streamingContext.start()
    // Driver进程阻塞等待采集器的执行
    streamingContext.awaitTermination()
  }
}
