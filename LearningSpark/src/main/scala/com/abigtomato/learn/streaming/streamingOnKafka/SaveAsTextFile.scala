package com.abigtomato.spark.scala.streaming.streamingOnKafka

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Durations, StreamingContext}

/**
  * 将SparkSteaming处理的结果保存在指定的目录中
  *
  * saveAsTextFiles(prefix, [suffix])：
  * 将此DStream的内容另存为文本文件。每批次数据产生的文件名称格式基于：prefix和suffix: "prefix-TIME_IN_MS[.suffix]".
  *
  * 注意：
  * saveAsTextFile是调用saveAsHadoopFile实现的
  * spark中普通rdd可以直接只用saveAsTextFile(path)的方式，保存到本地，但是此时DStream的只有saveAsTextFiles()方法，没有传入路径的方法，
  * 其参数只有prefix, suffix
  * 其实：DStream中的saveAsTextFiles方法中又调用了rdd中的saveAsTextFile方法，我们需要将path包含在prefix中
  */
object SaveAsTextFile {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setMaster("local")
    conf.setAppName("saveAsTextFile")
    val ssc = new StreamingContext(conf,Durations.seconds(5))
    /**
      * SparkStreaming 监控一个目录数据时
      *   1.这个目录下已经存在的文件不会被监控到，可以监控增加的文件
      *   2.增加的文件必须是原子性产生。
      */
    val lines: DStream[String] = ssc.textFileStream("./data/streamingCopyFile")
    val words: DStream[String] = lines.flatMap(line=>{line.split(" ")})
    val pairWords: DStream[(String, Int)] = words.map(word=>{(word,1)})
    val result: DStream[(String, Int)] = pairWords.reduceByKey((v1:Int, v2:Int)=>{v1+v2})
    //保存的多级目录就直接写在前缀中
    result.saveAsTextFiles("./data/streamingSavePath/spark/prefix","suffix")
    ssc.start()
    ssc.awaitTermination()
    ssc.stop()
  }
}
