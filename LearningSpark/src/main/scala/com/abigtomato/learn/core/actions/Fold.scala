package com.abigtomato.spark.scala.core.actions

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

/**
 * fold:
 *  - 简化aggregate的操作
 */
object Fold {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("Fold")
      .setMaster("local")
    val sc = new SparkContext(conf)

    val listRDD: RDD[Int] = sc.makeRDD(1 to 10, 2)

    // 每个分区内设置初值，分区间的计算也会设置初值
    val result: Int = listRDD.fold(10)(_ + _)

    println(result)

    sc.stop()
  }
}
