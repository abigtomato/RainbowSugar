package com.abigtomato.spark.scala.core.actions

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * aggregate:
 *  - 1.类似aggregateByKey
 *  - 2.可以作用在非k-v格式RDD上
 *  - 3.zeroValue同时作用在分区内部和外部
 */
object Aggregate {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("Aggregate")
      .setMaster("local")
    val sc = new SparkContext(conf)

    val listRDD: RDD[Int] = sc.makeRDD(1 to 10, 2)

    // 每个分区内设置初值，分区间的计算也会设置初值
    val result: Int = listRDD.aggregate(10)(_ + _, _ + _)

    println(result)

    sc.stop()
  }
}
