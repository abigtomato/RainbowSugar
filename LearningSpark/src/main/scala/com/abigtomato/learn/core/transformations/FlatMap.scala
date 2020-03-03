package com.abigtomato.spark.scala.core.transformations

import org.apache.spark.{SparkConf, SparkContext}

/**
  * flatMap：
  *   - 1.一对多的关系的映射；
  *   - 2.处理一条数据得到多条结果。
  */
object FlatMap {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("map")
      .setMaster("local")
    val sc = new SparkContext(conf)

    val infos = sc.parallelize(Array[String]("hello spark", "hello hdfs", "hello hive"))
    val result = infos.flatMap(one => one.split(" "))
    result.foreach(println)

    sc.stop()
  }
}
