package com.abigtomato.spark.scala.core.actions

import org.apache.spark.{SparkConf, SparkContext}

/**
  * collect：
  *   - 1.回收算子，会将结果回收到Driver端；
  *   - 2.如果结果比较大，就不要回收，这样的话会造成Driver端的OOM
  */
object Collect {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("Collect")
      .setMaster("local")
    val sc = new SparkContext(conf)

    val lines = sc.textFile("./data/words")
    val result: Array[String] = lines.collect()
    result.foreach(println)

    sc.stop()
  }
}

