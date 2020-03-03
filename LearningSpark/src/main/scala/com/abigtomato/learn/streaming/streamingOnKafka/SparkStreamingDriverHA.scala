package com.abigtomato.spark.scala.streaming.streamingOnKafka

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Durations, StreamingContext}

/**
  * Driver HA ：
  * 1.在提交application的时候  添加 --supervise 选项  如果Driver挂掉 会自动启动一个Driver
  * 2.代码层面恢复Driver(StreamingContext)
  *
  */
object SparkStreamingDriverHA {
  //设置checkpoint目录
  val ckDir = "./data/streamingCheckpoint"
  def main(args: Array[String]): Unit = {
    /**
      * StreamingContext.getOrCreate(ckDir,CreateStreamingContext)
      *   这个方法首先会从ckDir目录中获取StreamingContext【 因为StreamingContext是序列化存储在Checkpoint目录中，恢复时会尝试反序列化这些objects。
      *   如果用修改过的class可能会导致错误，此时需要更换checkpoint目录或者删除checkpoint目录中的数据，程序才能起来。】
      *
      *   若能获取回来StreamingContext,就不会执行CreateStreamingContext这个方法创建，否则就会创建
      */
    val ssc: StreamingContext = StreamingContext.getOrCreate(ckDir,CreateStreamingContext)
    ssc.start()
    ssc.awaitTermination()
    ssc.stop()

  }

  def CreateStreamingContext() = {
    println("=======Create new StreamingContext =======")
    val conf = new SparkConf()
    conf.setMaster("local")
    conf.setAppName("DriverHA")
    val ssc: StreamingContext = new StreamingContext(conf,Durations.seconds(5))

    /**
      *   默认checkpoint 存储：
      *     1.配置信息
      *   	2.DStream操作逻辑
      *   	3.batch执行的进度 或者【offset】
      */
    ssc.checkpoint(ckDir)
    val lines: DStream[String] = ssc.textFileStream("./data/streamingCopyFile")
    val words: DStream[String] = lines.flatMap(line=>{line.trim.split(" ")})
    val pairWords: DStream[(String, Int)] = words.map(word=>{(word,1)})
    val result: DStream[(String, Int)] = pairWords.reduceByKey((v1:Int, v2:Int)=>{v1+v2})

    result.print()

    /**
      * 更改逻辑
      */
//    result.foreachRDD(pairRDD=>{
//      pairRDD.filter(one=>{
//        println("*********** filter *********")
//        true
//      }).foreach(println)
//    })

    ssc
  }
}
