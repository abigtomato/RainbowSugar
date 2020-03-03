package com.abigtomato.spark.scala.core.actions

import org.apache.spark.{SparkConf, SparkContext}

/**
  * collectAsMap：将(K,V)格式的RDD回收到Driver端作为Map使用
  */
object CollectAsMap {
  def main(args: Array[String]): Unit = {
    val conf  = new SparkConf()
      .setMaster("local")
      .setAppName("collectAsMap")
    val sc = new SparkContext(conf)

    val weightInfos = sc.parallelize(List[(String, Double)](
      Tuple2("zhangsan", 78.4), Tuple2("lisi", 32.6), Tuple2("wangwu", 90.9)
    ))
    val stringToDouble: collection.Map[String, Double] = weightInfos.collectAsMap()
    stringToDouble.foreach(println)

    sc.stop()
  }
}
