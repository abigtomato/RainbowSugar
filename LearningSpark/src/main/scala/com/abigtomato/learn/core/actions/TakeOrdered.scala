package com.abigtomato.spark.scala.core.actions

import org.apache.spark.{SparkConf, SparkContext}

/**
 * takeOrdered：取出RDD中排序后的前N个元素
 */
object TakeOrdered {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("TakeOrdered")
      .setMaster("local")
    val sc = new SparkContext(conf)

    val lines = sc.makeRDD(Array(1, 4, 3, 2, 5))
    val array = lines.takeOrdered(3)
    array.foreach(println)

    sc.stop()
  }
}
