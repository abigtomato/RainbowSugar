package com.abigtomato.spark.scala.core.transformations

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * cartesian:
 *  - 取两个RDD的笛卡尔集
 */
object Cartesian {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("aggregateByKey")
      .setMaster("local")
    val sc = new SparkContext(conf)

    val listRDD1: RDD[Int] = sc.parallelize(1 to 3)
    val listRDD2: RDD[Int] = sc.parallelize(2 to 5)

    val cartesianRDD: RDD[(Int, Int)] = listRDD1.cartesian(listRDD2)

    cartesianRDD.foreach(println)

    sc.stop()
  }
}
