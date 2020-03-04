package com.abigtomato.spark.scala.streaming.streamingOnKafka

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Durations, StreamingContext}

/**
  * UpdateStateByKey 根据key更新状态
  *   1、为Spark Streaming中每一个Key维护一份state状态，state类型可以是任意类型的， 可以是一个自定义的对象，那么更新函数也可以是自定义的。
  *   2、通过更新函数对该key的状态不断更新，对于每个新的batch而言，Spark Streaming会在使用updateStateByKey的时候为已经存在的key进行state的状态更新
  */
object UpdateStateByKey {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setMaster("local[2]")
    conf.setAppName("UpdateStateByKey")
    val ssc = new StreamingContext(conf,Durations.seconds(5))
    //设置日志级别
    ssc.sparkContext.setLogLevel("ERROR")
    val lines: ReceiverInputDStream[String] = ssc.socketTextStream("node5",9999)
    val words: DStream[String] = lines.flatMap(line=>{line.split(" ")})
    val pairWords: DStream[(String, Int)] = words.map(word => {(word, 1)})

    /**
      * 根据key更新状态，需要设置 checkpoint来保存状态
      * 默认key的状态在内存中 有一份，在checkpoint目录中有一份。
      *
      *    多久会将内存中的数据（每一个key所对应的状态）写入到磁盘上一份呢？
      * 	      如果你的batchInterval小于10s  那么10s会将内存中的数据写入到磁盘一份
      * 	      如果bacthInterval 大于10s，那么就以bacthInterval为准
      *
      *    这样做是为了防止频繁的写HDFS
      */
    ssc.checkpoint("./data/streamingCheckpoint")
//    ssc.sparkContext.setCheckpointDir("./data/streamingCheckpoint")
    /**
      * currentValues :当前批次某个 key 对应所有的value 组成的一个集合
      * preValue : 以往批次当前key 对应的总状态值
      */
    val result: DStream[(String, Int)] = pairWords.updateStateByKey((currentValues: Seq[Int], preValue: Option[Int]) => {
      var totalValues = 0
      if (!preValue.isEmpty) {
        totalValues += preValue.get
      }
      for(value <- currentValues){
        totalValues += value
      }

      Option(totalValues)
    })
    result.print()
    ssc.start()
    ssc.awaitTermination()
    ssc.stop()

  }
}
