package com.abigtomato.spark.scala.core.transformations

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * groupBy:
 *  - 分组，按照传入函数的返回值进行分组，将相同key对应的值放入一个迭代器
 */
object GroupBy {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setMaster("local")
      .setAppName("groupByKey")
    val sc = new SparkContext(conf)

    val listRDD: RDD[Int] = sc.parallelize(1 to 4)

    val groupByRDD: RDD[(Int, Iterable[Int])] = listRDD.groupBy(i => i % 2)

    groupByRDD.foreach(println)

    sc.stop()
  }
}
