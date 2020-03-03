package com.abigtomato.spark.scala.core.actions

import org.apache.spark.{SparkConf, SparkContext}

/**
  * count：统计RDD共有多少行数据
  */
object Count {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("count")
      .setMaster("local")
    val sc = new SparkContext(conf)

    val lines = sc.textFile("./data/sampleData.txt")
    val result: Long = lines.count()
    println(result)

    sc.stop()
  }
}
