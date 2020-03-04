package com.abigtomato.spark.scala.core.transformations

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * sample:
 *  - 随机抽样算子
 *  - 参数1：有无放回的抽样
 *  - 参数2：分值
 *  - 参数3：随机数种子
 */
object Sample {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setMaster("local")
      .setAppName("sample")
    val sc = new SparkContext(conf)

    val listRDD: RDD[Int] = sc.parallelize(1 to 10)

    val sampleRDD: RDD[Int] = listRDD.sample(withReplacement = false, 0.4, 1)

    sampleRDD.foreach(println)

    sc.stop()
  }
}
