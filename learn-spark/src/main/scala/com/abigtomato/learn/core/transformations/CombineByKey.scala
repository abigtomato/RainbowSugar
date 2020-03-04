package com.abigtomato.spark.scala.core.transformations

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ArrayBuffer

/**
 * combineByKey：
 *  - 1.首先给RDD中每个分区中的每一个值执行操作；
 *  - 2.然后在RDD每个分区内部，相同的key执行操作；
 *  - 3.最后在RDD不同的分区之间将相同的key结果再执行操作。
 */
object CombineByKey {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setMaster("local")
      .setAppName("CombineByKey")
    val sc = new SparkContext(conf)

    val rdd1: RDD[(String, Int)] = sc.makeRDD(List[(String, Int)](
      ("zhangsan", 10), ("zhangsan", 20), ("wangwu", 30),
      ("lisi", 40), ("zhangsan", 50), ("lisi", 60),
      ("wangwu", 70), ("wangwu", 80), ("lisi", 90)
    ),
      3)
    rdd1.mapPartitionsWithIndex((index,iter)=>{
      val arr = ArrayBuffer[(String,Int)]()
      iter.foreach(tp=>{
        arr.append(tp)
        println("rdd1 partition index = "+index+",value = "+tp)
      })
      arr.iterator
    }).count()

    /**
      * 0号分区：("zhangsan", 10), ("zhangsan", 20), ("wangwu", 30)
      * 1号分区：("lisi", 40), ("zhangsan", 50), ("lisi", 60)
      * 2号分区：("wangwu", 70), ("wangwu", 80), ("lisi", 90)
      *
      * 初始化后：
      * 0号分区：("zhangsan", 10hello), ("wangwu", 30hello)
      * 1号分区：("lisi", 40hello), ("zhangsan", 50hello)
      * 2号分区：("wangwu", 70hello), ("lisi", 90hello)
      *
      * 经过RDD分区内的合并后:
      * 0号分区：("zhangsan", 10hello@20)，("wangwu", 30hello)
      * 1号分区：("lisi", 40hello@60), ("zhangsan", 50hello)
      * 2号分区：("wangwu", 70hello@80),("lisi", 90hello)
      *
      * 经过RDD分区之间的合并：
      * ("zhangsan", 10hello@20#50hello), ("lisi",40hello@60#90hello), ("wangwu", 30hello#70hello@80)
      */
    val result: RDD[(String, String)] = rdd1.combineByKey(v => v + "hello", (s, v) => s + "@" + v, (s1, s2) => s1 + "#" + s2)
    result.foreach(println)

    /**
     * 计算RDD中每种key的平均值
     *  - createCombiner: 针对每个分区中的每个key执行的操作，设定key的初始结构（每种key调用一次）
     *  - mergeValue: 针对分区内数据执行的操作，相同key做操作
     *  - mergeCombiners: 针对所有分区间的数据执行的操作，相同key做操作
     */
    val arrRDD: RDD[(String, Int)] = sc.parallelize(Array(("a", 88), ("b", 95), ("a", 91), ("b", 93), ("a", 95), ("b", 98)), 2)

    val combineByKeyRDD = arrRDD.combineByKey(v => (v, 1),
      (v1: (Int, Int), v2) => (v1._1 + v2, v1._2 + 1),  // 这里第一个参数是key的初始结构，第二个参数是相同key的其他值
      (v1: (Int, Int), v2: (Int, Int)) => (v1._1 + v2._1, v1._2 + v2._2))
    combineByKeyRDD.foreach(println)

    combineByKeyRDD.map{case (key, value) => (key, value._1 / value._2)}.foreach(println)

    sc.stop()
  }
}
