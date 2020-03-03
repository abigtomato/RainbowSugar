package com.abigtomato.spark.scala.core.examples

import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

object PvUv {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setMaster("local")
      .setAppName("pvuv")
    val sc = new SparkContext(conf)

    val lines = sc.textFile("./data/pvuvdata")

    // 1.pv
    lines.map(one => (one.split("\t")(5), 1))
      .reduceByKey((v1, v2) => v1 + v2)
      .sortBy(tp => tp._2, ascending = false)
      .foreach(println)

    // 2.uv
    lines.map(one => s"${one.split("\t")(0)}_${one.split("\t")(5)}")
      .distinct()
      .map(one => (one.split("_")(1), 1))
      .reduceByKey(_+_)
      .sortBy(_._2)
      .foreach(println)

    // 3.计算每个网址，最活跃的网址及对应的人数
    lines.map(one => (one.split("\t")(5), one.split("\t")(1)))
      .groupByKey()
      .map(tp => {
        val site = tp._1
        val localIterator = tp._2.iterator
        val localMap = mutable.Map[String, Int]()

        while (localIterator.hasNext) {
          val currentLocal = localIterator.next()
          if (localMap.contains(currentLocal)) {
            val count = localMap(currentLocal) + 1
            localMap.put(currentLocal, count)
          } else {
            localMap.put(currentLocal, 1)
          }
        }

        val newList: List[(String, Int)] = localMap.toList.sortBy(tp => -tp._2) // 对Map排序
        (site, newList.toBuffer)
      }).foreach(println)

    sc.stop()
  }
}
