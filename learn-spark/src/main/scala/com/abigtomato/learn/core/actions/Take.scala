package com.abigtomato.spark.scala.core.actions

import org.apache.spark.{SparkConf, SparkContext}

/**
 * take：取出RDD中的前N个元素
 */
object Take {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("Take")
      .setMaster("local")
    val sc = new SparkContext(conf)

    val lines = sc.makeRDD(Array(1, 4, 3, 2, 5))
    val array = lines.take(3)
    array.foreach(println)

    sc.stop()
  }
}
