package com.abigtomato.spark.scala.core.transformations

import org.apache.spark.{SparkConf, SparkContext}

/**
 * foldByKey:
 *  - aggregateByKey的简化操作，分区内操作函数和分区间操作函数相同
 */
object FoldByKey {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("FoldByKey")
      .setMaster("local")
    val sc = new SparkContext(conf)

    /**
     * foldByKey版wordcount
     */
    sc.textFile("src/main/resources/data/WordCount.csv")
      .flatMap(line => line.split(" "))
      .map(word => (word, 1))
      .foldByKey(0)(_+_)
      .sortByKey(ascending = false)
      .foreach(println)

    sc.stop()
  }
}
