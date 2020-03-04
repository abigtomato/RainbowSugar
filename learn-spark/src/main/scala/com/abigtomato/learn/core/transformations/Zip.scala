package com.abigtomato.spark.scala.core.transformations

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * zip:
 *  - 将两个RDD组合成k-v格式的RDD
 *  - 默认分区数量和元素数量都要相同
 */
object Zip {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setMaster("local")
      .setAppName("zip")
    val sc = new SparkContext(conf)

    val rdd1: RDD[Int] = sc.parallelize(1 to 3, 3)
    val rdd2: RDD[String] = sc.parallelize(Array("a", "b", "c"), 3)

    val zipRDD: RDD[(Int, String)] = rdd1.zip(rdd2)

    zipRDD.foreach(println)

    sc.stop()
  }
}
