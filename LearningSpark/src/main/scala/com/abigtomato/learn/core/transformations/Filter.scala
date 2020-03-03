package com.abigtomato.spark.scala.core.transformations

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * filter:
 *  - 按照指定的规则过滤出数据
 */
object Filter {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setMaster("local")
      .setAppName("groupByKey")
    val sc = new SparkContext(conf)

    val listRDD: RDD[Int] = sc.parallelize(1 to 4)

    val filterRDD: RDD[Int] = listRDD.filter(x => x % 2 == 0)

    filterRDD.foreach(println)

    sc.stop()
  }
}
