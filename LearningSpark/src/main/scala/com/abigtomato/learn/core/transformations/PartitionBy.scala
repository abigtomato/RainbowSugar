package com.abigtomato.spark.scala.core.transformations

import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

/**
 * partitionBy:
 *  - 自定义分区器进行分区
 */
object PartitionBy {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("PartitionBy")
      .setMaster("local")
    val sc = new SparkContext(conf)

    val listRDD: RDD[(Int, String)] = sc.parallelize(Array((1, "aaa"), (2, "bbb"), (3, "ccc"), (4, "ddd")), 4)
    println(listRDD.partitions.length)

    val partitionByRDD: RDD[(Int, String)] = listRDD.partitionBy(new HashPartitioner(2))
    println(partitionByRDD.partitions.length)

    sc.stop()
  }
}
