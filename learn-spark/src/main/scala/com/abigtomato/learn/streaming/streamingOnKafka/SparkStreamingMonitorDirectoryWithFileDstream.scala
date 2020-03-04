package com.abigtomato.spark.scala.streaming.streamingOnKafka

import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.{Durations, StreamingContext}

object SparkStreamingMonitorDirectoryWithFileDstream {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setMaster("local")
    conf.setAppName("test")
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    val ssc = new StreamingContext(conf,Durations.seconds(5))
    ssc.sparkContext.setLogLevel("Error")

    val value: InputDStream[(LongWritable, Text)] =
      ssc.fileStream[LongWritable, Text, TextInputFormat]("hdfs://mynode1:8020/spark/data/",(path:Path)=>{path.getName.endsWith("_COPYING_")},true)
    value.map(one=>{
      one._2
    }).foreachRDD(rdd=>{
      println("++++++++++++++++++")
      rdd.foreach(println)
    })

    ssc.start()
    ssc.awaitTermination()

  }
}
