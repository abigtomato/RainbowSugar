package com.abigtomato.spark.scala.core.transformations

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

/**
  * intersection：
  *   - 1.取两个RDD的交集，两个RDD的类型要一致；
  *   - 2.结果RDD的分区数与父RDD的一致。
  */
object Intersection {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setMaster("local")
      .setAppName("intersection")
    val sc = new SparkContext(conf)

    val rdd1 = sc.parallelize(List[String]("zhangsan", "lisi", "wangwu"), 5)
    val rdd2 = sc.parallelize(List[String]("zhangsan", "lisi", "maliu"), 4)
    val intersectionRDD: RDD[String] = rdd1.intersection(rdd2)
    intersectionRDD.foreach(println)
    println("intersectionRDD partition length = " + intersectionRDD.getNumPartitions)

    sc.stop()
  }
}
