package com.abigtomato.spark.scala.core.examples

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

/**
 * WordCount：词频统计案例
 */
object WordCount {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
    val path = "file:///D:\\WorkSpace\\javaproject\\learn-spark\\src\\main\\resources\\WordCount.csv"

    val sc = SparkSession
      .builder()
      .master("local[3]")
      .appName("WordCount")
      .getOrCreate()
      .sparkContext

    /**
     * 1.最简化写法
     */
    sc.textFile(path)
      .flatMap{_.split(" ")}
      .map{(_, 1)}
      .reduceByKey{_+_}
      .sortBy{_._2}
      .saveAsTextFile("target/wc/result")

    /**
     * 2.简化写法
     */
    sc.textFile(path)
      .flatMap(line => line.split(" "))
      .map(word => (word, 1))
      .reduceByKey((v1, v2) => v1 + v2, numPartitions = 1)
      .sortBy(tuple => tuple._2)
      .foreach(tuple => println(tuple))

    /**
     * 3.详细写法
     */
    val count = sc.textFile(path)
      .flatMap((line: String) => {
        line.split(" ")
      }).map((word: String) => {
        Tuple2(word, 1)
      }).reduceByKey((v1: Int, v2: Int) => {
        v1 + v2
      }, numPartitions = 1).sortBy((tuple: (String, Int)) => {
        tuple._2
      }, ascending = false).count
    println(s"result count: $count")
  }
}
