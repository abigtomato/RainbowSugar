package com.abigtomato.spark.scala.core.other

import org.apache.spark.util.LongAccumulator
import org.apache.spark.{SparkConf, SparkContext}

/**
 * 累加器
 */
object Accumulator {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setMaster("local")
      .setAppName("accumulator")
    val sc = new SparkContext(conf)

    // 定义一个long类型的累加器变量
    val accumulator: LongAccumulator = sc.longAccumulator("My Accumulator")

    sc.textFile("./data/words").map(line => {
      accumulator.add(1)
      line
    }).collect()
    println(s"accumulator value is ${accumulator.value}")

    sc.stop()
  }
}
