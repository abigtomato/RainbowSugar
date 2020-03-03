package com.abigtomato.spark.scala.core.actions

import org.apache.spark.{SparkConf, SparkContext}

/**
 * reduce：将RDD中的每个元素按照指定规则聚合
 */
object Reduce {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("countByKey")
      .setMaster("local")
    val sc = new SparkContext(conf)

    val rdd = sc.makeRDD(Array[Int](1, 2, 3, 4, 5))
    val result: Int = rdd.reduce((v1, v2) => v1 + v2)
    println(result)

    sc.stop()
  }
}
