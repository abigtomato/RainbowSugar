package com.abigtomato.spark.scala.core.transformations

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ArrayBuffer

/**
 * aggregateByKey：
 *  - 1.首先是给定RDD的每个分区一个初始值；
 *  - 2.然后RDD各分区内部按照相同的key，结合初始值去合并；
 *  - 3.最后RDD各分区按照相同的key聚合。
 */
object AggregateByKey {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("aggregateByKey")
      .setMaster("local")
    val sc = new SparkContext(conf)

    val listRDD = sc.makeRDD(List[(String, Int)](
      ("zhangsan", 10), ("zhangsan", 20), ("wangwu", 30),
      ("lisi", 40), ("zhangsan", 50), ("lisi", 60),
      ("wangwu", 70), ("wangwu", 80), ("lisi", 90)
    ), 3)

    listRDD.mapPartitionsWithIndex((index, iter) => {
        val arr = ArrayBuffer[(String, Int)]()
        iter.foreach(tp => {
          arr.append(tp)
          println("rdd1 partition index = " + index + ", value = " + tp)
        })
       arr.iterator
    }).count()

    /**
     * 0号分区：
     *  ("zhangsan", 10) ("zhangsan", 20) ("wangwu", 30)
     * 1号分区：
     *  ("lisi", 40) ("zhangsan", 50) ("lisi", 60)
     * 2号分区：
     *  ("wangwu", 70) ("wangwu", 80) ("lisi", 90)
     *
     * RDD各分区内部合并：
     *  0:("zhangsan", hello~10~20), ("wangwu", hello~30)
     *  1:("zhangsan", hello~50), ("lisi", hello~40~60)
     *  2:("lisi", hello~90), ("wangwu", hello~70~80)
     *
     * RDD各分区合并：
     *  ("zhangsan", hello~10~20#hello~50), ("lisi", hello~40~60#hello~90), ("wangwu", hello~30#hello~70~80)
     */
    val result: RDD[(String, String)] = listRDD.aggregateByKey("hello")((s, v) => s + "~" + v, (s1, s2) => s1 + "#" + s2)
    result.foreach(print)

    /**
     * 取出每个分区相同key对应值的最大值，然后相加
     *  - zeroValue: 给每一个分区中每一个key一个初始值
     *  - seqOp: 基于初始值操作每个分区中的数据
     *  - combOp: 操作分区间的数据
     */
    val kvRDD: RDD[(String, Int)] = sc.parallelize(List(("a", 3), ("a", 2), ("c", 4), ("b", 3), ("c", 6), ("c", 8)), 2)

    val aggRDD: RDD[(String, Int)] = kvRDD.aggregateByKey(0)((v1, v2) => math.max(v1, v2), (v1, v2) => v1 + v2)
    aggRDD.foreach(println)

    /**
     * aggregateByKey版wordcount
     */
    val wordCountRDD: RDD[(String, Int)] = kvRDD.aggregateByKey(0)(_+_, _+_)
    wordCountRDD.foreach(println)

    sc.stop()
  }
}
