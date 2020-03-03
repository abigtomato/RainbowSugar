package com.abigtomato.spark.scala.core.transformations

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

/**
  * flatMapValues：
  *   - 1.(K, V) -> (K, V)
  *   - 2.作用在(K,V)格式的RDD上，对一个Key的一个Value返回多个Value
  */
object FlatMapValues {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("flatMapValues")
      .setMaster("local")
    val sc = new SparkContext(conf)

    val infos: RDD[(String, String)] = sc.makeRDD(List[(String, String)](("zhangsna", "18"), ("lisi", "20"), ("wangwu", "30")))
    val transInfo: RDD[(String, String)] = infos.mapValues(s => s + " " + "zhangsan18")
    val result = transInfo.flatMapValues(s => s.split(" "))
    result.foreach(print)

    sc.stop()
  }
}
