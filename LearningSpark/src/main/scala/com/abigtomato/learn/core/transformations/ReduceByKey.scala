package com.abigtomato.spark.scala.core.transformations

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * reduceByKey:
 *  - 作用于k-v格式的RDD上，将相同key的值进行自定义聚合
 * reduceByKey和groupByKey的区别:
 *  - 1.reduceByKey按照key进行聚合，在shuffle之前有combine（预聚合）操作，返回结果是RDD[k, v]
 *  - 2.groupByKey按照key进行分组，直接进行shuffle
 */
object ReduceByKey {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("ReduceByKey")
      .setMaster("local")
    val sc = new SparkContext(conf)

    val listRDD: RDD[(String, Int)] = sc.parallelize(List(("female", 1), ("male", 5), ("female", 5), ("male", 2)))

    val reduceByKeyRDD: RDD[(String, Int)] = listRDD.reduceByKey((x, y) => x + y)

    reduceByKeyRDD.foreach(println)

    sc.stop()
  }
}
