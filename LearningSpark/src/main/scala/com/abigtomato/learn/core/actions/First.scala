package com.abigtomato.spark.scala.core.actions

import org.apache.spark.{SparkConf, SparkContext}

/**
 * first：取出第一个元素
 */
object First {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setMaster("local")
      .setAppName("first")
    val sc = new SparkContext(conf)

    val lines = sc.textFile("./data/words")
    val str = lines.first()
    println(str)

    sc.stop()
  }
}
