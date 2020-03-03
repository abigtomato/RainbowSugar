package com.abigtomato.spark.scala.core.transformations

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * glom：
 *  - 将每个分区的数据都转换成数组中
 */
object Glom {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("glom")
      .setMaster("local")
    val sc = new SparkContext(conf)

    val listRDD: RDD[Int] = sc.makeRDD(1 to 17, 4)

    val glomRDD: RDD[Array[Int]] = listRDD.glom()

    glomRDD.foreach(array => println(array.mkString(",")))

    sc.stop()
  }
}
