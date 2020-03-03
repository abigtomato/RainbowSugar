package com.abigtomato.spark.scala.core.transformations

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * distinct：
  *   - 1.去重；
  *   - 2.会产生shuffle；
  *   - 3.内部实际是map + reduceByKey + map实现。
  */
object Distinct {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setMaster("local")
      .setAppName("distinct")
    val sc = new SparkContext(conf)

    val infos = sc.parallelize(List[String]("a", "a", "b", "b", "c", "c", "d"), 4)
    val result: RDD[String] = infos.distinct()
    result.foreach(println)

    sc.stop()
  }
}
