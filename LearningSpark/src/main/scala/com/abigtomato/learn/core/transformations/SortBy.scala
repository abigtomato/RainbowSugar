package com.abigtomato.spark.scala.core.transformations

import org.apache.spark.{SparkConf, SparkContext}

/**
  * sortBy:
  *   - 1.排序，参数中指定按照什么规则去排序，第二个参数(true/false)指定升序或者降序；
  *   - 2.无需作用在(K,V)格式的RDD上。
  */
object SortBy {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("sortBy")
      .setMaster("local")
    val sc = new SparkContext(conf)

    sc.parallelize(Array[(String, String)](("f", "f"), ("a", "a"), ("c", "c"), ("b", "b")))
      .sortBy(tp => tp._1, ascending = false)
      .foreach(println)

    sc.parallelize(Array[Int](400, 200, 500, 100, 300))
      .sortBy(one => one / 100, ascending = false)
      .foreach(println)

    sc.stop()
  }
}
