package com.abigtomato.spark.scala.core.transformations

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * union：
  *   - 1.合并两个RDD；
  *   - 2.合并的两个RDD必须是同种类型，不必要是(K,V)格式的RDD。
  */
object Union {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setMaster("local")
      .setAppName("union")
    val sc = new SparkContext(conf)

    val rdd1 = sc.parallelize(List[String]("zhangsan", "lisi", "wangwu", "maliu"), 3)
    val rdd2 = sc.parallelize(List[String]("a", "b", "c", "d"), 4)
    val unionRDD: RDD[String] = rdd1.union(rdd2)
    unionRDD.foreach(println)
    println("unionRDD partitioin length = " + unionRDD.getNumPartitions)

    sc.stop()
  }
}
