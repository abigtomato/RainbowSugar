package com.abigtomato.spark.scala.streaming.streamingOnKafka

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Durations, StreamingContext}

/**
  * SparkStreaming 注意：
  * 1.需要设置local[2]，因为一个线程是读取数据，一个线程是处理数据
  * 2.创建StreamingContext两种方式，采用的是StreamingContext(SparkConf/SparkContext,Durations.seconds(5))两种方式
  * 3.Durations 批次间隔时间的设置需要根据集群的资源情况以及监控每一个job的执行时间来调节出最佳时间。
  * 4.SparkStreaming所有业务处理完成之后需要有一个output operato操作
  * 5.StreamingContext.start()straming框架启动之后是不能在次添加业务逻辑
  * 6.StreamingContext.stop()无参的stop方法会将sparkContext一同关闭，stop(false) ,默认为true，会一同关闭
  * 7.StreamingContext.stop()停止之后是不能再调用start
  */
object WordCountFromSocket {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setAppName("WordCountOnLine").setMaster("local[2]")
    val ssc = new StreamingContext(conf,Durations.seconds(5))
    ssc.sparkContext.setLogLevel("ERROR")
    //使用new StreamingContext(conf,Durations.seconds(5)) 这种方式默认会创建SparkContext
//    val sc = new SparkContext(conf)
    //从ssc中获取SparkContext()
//    val context: SparkContext = ssc.sparkContext

    val lines: ReceiverInputDStream[String] = ssc.socketTextStream("mynode5",9999)
    val words: DStream[String] = lines.flatMap(line=>{line.split(" ")})
    val pairWords: DStream[(String, Int)] = words.map(word=>{(word,1)})
    val result: DStream[(String, Int)] = pairWords.reduceByKey((v1, v2)=>{v1+v2})

    result.print()

    /**
      * foreachRDD 注意事项：
      * 1.foreachRDD中可以拿到DStream中的RDD，对RDD进行操作，但是一点要使用RDD的action算子触发执行，不然DStream的逻辑也不会执行
      * 2.froeachRDD算子内，拿到的RDD算子操作外，这段代码是在Driver端执行的，可以利用这点做到动态的改变广播变量
      *
      */
//    result.foreachRDD(wordCountRDD=>{
//
//      println("******* produce in Driver *******")
//
//      val sortRDD: RDD[(String, Int)] = wordCountRDD.sortByKey(false)
//      val result: RDD[(String, Int)] = sortRDD.filter(tp => {
//        println("******* produce in Executor *******")
//        true
//      })
//      result.foreach(println)
//    })

    ssc.start()
    ssc.awaitTermination()
    ssc.stop(false)

  }
}
