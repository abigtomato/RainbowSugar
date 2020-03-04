package com.abigtomato.spark.scala.streaming.streamingOnKafka

import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.ReceiverInputDStream
import org.apache.spark.streaming.{Durations, StreamingContext}
import org.apache.spark.util.LongAccumulator
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 在Sparkstreaming中使用累加器
  *
  * 1.利用累加器统计自从SparkStreaming启动以来所有单词个数
  * 2.利用累加器统计每批次 “zhangsan” 个数
  * 3.统计每批次 wordCount 总数
  */
object AccumulatorInSparkStreaming {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setAppName("WordCountOnLine").setMaster("local[2]")
    val ssc = new StreamingContext(conf,Durations.seconds(5))
    ssc.sparkContext.setLogLevel("ERROR")
    val globalAcc: LongAccumulator = ssc.sparkContext.longAccumulator("globalAcc")

    val lines: ReceiverInputDStream[String] = ssc.socketTextStream("mynode5",9999)
    val result = lines.flatMap(line=>{
      line.split(" ")
    })
    result.foreachRDD(wordCountRDD=>{
      val sc: SparkContext = wordCountRDD.context
      val myacc: LongAccumulator = sc.longAccumulator("myacc")
      val value: RDD[String] = wordCountRDD.map(word => {
        if ("zhangsan".equals(word)) {
          myacc.add(1)
        }
        globalAcc.add(1)
        word
      })
      val batchWordCount = value.count()
      println(s"accumulator value【张三count】 = ${myacc.count} , batchTotalCount = ${batchWordCount} ,globalAcc = ${globalAcc.value}")

      println("会处理完当前逻辑后再停止SparkStreamingContext")
    })

    ssc.start()
    ssc.awaitTermination()
  }
}
