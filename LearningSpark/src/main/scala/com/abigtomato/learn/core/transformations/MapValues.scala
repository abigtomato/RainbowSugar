package com.abigtomato.spark.scala.core.transformations

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * mapValues:
 *  - 针对k-v格式的RDD，对每个value进行操作
 */
object MapValues {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("MapValues")
      .setMaster("local")
    val sc = new SparkContext(conf)

    val arrRDD: RDD[(Int, String)] = sc.parallelize(Array((1, "a"), (1, "d"), (2, "b"), (3, "c")))

    val mapValuesRDD: RDD[(Int, String)] = arrRDD.mapValues(_ + "-")

    mapValuesRDD.foreach(println)

    sc.stop()
  }
}
