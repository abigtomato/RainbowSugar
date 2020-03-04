package com.abigtomato.spark.scala.core.transformations

import org.apache.spark.{SparkConf, SparkContext}

/**
  * groupByKey：
  *   - 1.根据key将相同的key对应的value合并在一起；
  *   - 2.（K, V） => (K, [V])
  */
object GroupByKey {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setMaster("local")
      .setAppName("groupByKey")
    val sc = new SparkContext(conf)

    val rdd = sc.parallelize(List[(String,Double)](
      ("zhangsan", 66.5), ("lisi", 33.2), ("zhangsan", 66.7),
      ("lisi", 33.4), ("zhangsan", 66.8), ("wangwu", 29.8)))

    rdd.groupByKey().foreach(info => {
      val name = info._1
      val value: Iterable[Double] = info._2
      val list: List[Double] = value.toList
      print("name = " + name + ", list = " + list)
    })

    sc.stop()
  }
}
