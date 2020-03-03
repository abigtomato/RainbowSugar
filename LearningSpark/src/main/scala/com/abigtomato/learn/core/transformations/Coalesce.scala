package com.abigtomato.spark.scala.core.transformations

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.{ArrayBuffer, ListBuffer}

/**
 * coalesce(numPartition, shuffle=false)：
 *  - 1.增加或者减少分区，默认不发生shuffle；
 *  - 2.如果从少的分区增加到多的分区，必须要发生shuffle（如果指定没有shuffle，那么不起作用）；
 *  - 3.coalesce(num, true) = repartition(num)。
 */
object Coalesce {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setMaster("local")
      .setAppName("coalesce")
    val sc = new SparkContext(conf)

    val rdd1: RDD[String] = sc.parallelize(List[String](
      "love1", "love2", "love3", "love4",
      "love5", "love6", "love7", "love8",
      "love9", "love10", "love11", "love12"
    ),3)

    val rdd2 :RDD[String] = rdd1.mapPartitionsWithIndex((index, iter) => {
      val list = ListBuffer[String]()
      iter.foreach(one => {
        list.append(s"rdd1 partition = [$index], value = [$one]")
      })
      list.iterator
    }, preservesPartitioning = true)

    val rdd3 = rdd2.coalesce(4, shuffle = false)

    val rdd4 = rdd3.mapPartitionsWithIndex((index,iter)=>{
      val arr = ArrayBuffer[String]()
      iter.foreach(one=>{
        arr.append(s"rdd3 partition = [$index], value = [$one]")
      })
      arr.iterator
    })

    val results : Array[String] = rdd4.collect()
    results.foreach(println)

    sc.stop()
  }
}
