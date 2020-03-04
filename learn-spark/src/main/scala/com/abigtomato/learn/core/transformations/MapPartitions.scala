package com.abigtomato.spark.scala.core.transformations

import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ArrayBuffer

/**
  * mapPartitions：
  *   - 1.遍历的是每个分区中的数据，以分区为维度的遍历；
  *   - 2.相对于map的一条一条处理数据，性能比较高。
  */
object MapPartitions {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setMaster("local")
      .setAppName("mapPartitions")
    val sc = new SparkContext(conf)

    val infos = sc.parallelize(List[String]("a", "b", "c", "d", "e", "f", "g"), 4)

    infos.mapPartitions(iter => {
      println("创建数据库连接... ...")
      val array = ArrayBuffer[String]()
      while (iter.hasNext) {
        val s = iter.next()
        println("拼接sql......" + s)
        array.append(s)
      }
      println("关闭数据库连接......")
      array.iterator
    }).count()

    sc.stop()
  }
}
