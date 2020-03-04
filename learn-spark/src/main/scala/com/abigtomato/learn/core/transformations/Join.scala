package com.abigtomato.spark.scala.core.transformations

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * join：
  *   - 1.会产生shuffle；
  *   - 2.(K, V)格式的RDD和(K, V)格式的RDD按照key相同join得到(K, (V, W))格式的数据。
  */
object Join {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("join")
      .setMaster("local")
    val sc = new SparkContext(conf)

    val nameRDD = sc.parallelize(List[(String, String)](("zhangsan", "female"), ("lisi", "male"), ("wangwu", "female")))
    val scoreRDD = sc.parallelize(List[(String, Int)](("zhangsan", 18), ("lisi", 19), ("wangwu", 20)))
    val joinRDD: RDD[(String, (String, Int))] = nameRDD.join(scoreRDD)
    joinRDD.foreach(println)

    sc.stop()
  }
}
