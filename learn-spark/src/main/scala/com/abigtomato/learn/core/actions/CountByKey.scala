package com.abigtomato.spark.scala.core.actions

import org.apache.spark.{SparkConf, SparkContext}

/**
  * countByKey：统计相同的key出现的个数
  */
object CountByKey {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("countByKey")
      .setMaster("local")
    val sc = new SparkContext(conf)

    val rdd  = sc.makeRDD(List[(String, Integer)](("a", 1), ("a", 100), ("a", 1000), ("b", 2), ("b", 200), ("c", 3)))
    val result: collection.Map[String, Long] = rdd.countByKey()
    result.foreach(print)

    sc.stop()
  }
}
