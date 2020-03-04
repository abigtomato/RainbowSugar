package com.abigtomato.spark.scala.core.actions

import org.apache.spark.{SparkConf, SparkContext}

/**
  * takeSample(withReplacement, num, seed)：
  *   - 1.随机抽样将数据结果拿回Driver端使用，返回Array；
  *   - 2.withReplacement: 有无放回的抽样；
  *   - 3.num: 抽样的条数；
  *   - 4.seed: 种子，保证每次抽样出来的都是相同的数据。
  */
object TakeSample {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setMaster("local")
      .setAppName("takeSample")
    val sc = new SparkContext(conf)

    val lines = sc.textFile("./data/words")
    val result: Array[String] = lines.takeSample(withReplacement = false, 10, 100)
    result.foreach(println)

    sc.stop()
  }
}
