package com.abigtomato.spark.scala.core.actions

import org.apache.spark.{SparkConf, SparkContext}

/**
 * foreachPartition：以分区为单位遍历RDD中的每个元素
 */
object ForeachPartition {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setMaster("local")
      .setAppName("foreachPartition")
    val sc = new SparkContext(conf)

    val infos = sc.parallelize(List[String]("a", "b", "c", "d", "e", "f", "g"), 4)
    infos.foreachPartition(println)

    sc.stop()
  }
}
