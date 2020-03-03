package com.abigtomato.spark.scala.core.transformations

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

/**
  * subtract：
  *   - 1.取两个RDD的差集
  *   - 2.subtract两边RDD的类型要一致，结果RDD的分区数与subtract前RDD的分区个数一致。
  */
object Subtract {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setMaster("local")
      .setAppName("subtract")
    val sc = new SparkContext(conf)

    val rdd1 = sc.parallelize(List[String]("zhangsan", "lisi", "wangwu"), 5)
    val rdd2 = sc.parallelize(List[String]("zhangsan", "lisi", "maliu"), 4)
    val subtractRDD: RDD[String] = rdd1.subtract(rdd2)
    subtractRDD.foreach(println)
    println("subtractRDD partition length = " + subtractRDD.getNumPartitions)

    sc.stop()
  }
}
