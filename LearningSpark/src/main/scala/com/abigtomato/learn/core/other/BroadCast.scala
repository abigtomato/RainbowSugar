package com.abigtomato.spark.scala.core.other

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * 广播变量
 */
object BroadCast {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setMaster("local")
      .setAppName("broadCast")
    val sc = new SparkContext(conf)

    val list = List[String]("zhangsan", "tianqi")

    // 将list置为广播变量
    val blackList: Broadcast[List[String]] = sc.broadcast(list)

    val nameRDD: RDD[String] = sc.parallelize(List[String]("zhangsan", "lisi", "wangwu", "zhaoliu", "tianqi"))
    nameRDD.filter(name => !blackList.value.contains(name)).foreach(println)

    sc.stop()
  }
}
