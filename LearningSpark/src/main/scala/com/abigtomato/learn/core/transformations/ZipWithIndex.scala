package com.abigtomato.spark.scala.core.transformations

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * zipWithIndex:
 *  - 将RDD和数据下标压缩成一个(K,V)格式的RDD
 */
object ZipWithIndex {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setMaster("local")
      .setAppName("zipWithIndex")
    val sc = new SparkContext(conf)

    val listRDD: RDD[String] = sc.parallelize(List[String]("a", "b", "c"), 2)
    val result: RDD[(String, Long)] = listRDD.zipWithIndex()
    result.foreach(print)

    sc.stop()
  }
}
