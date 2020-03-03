package com.abigtomato.spark.scala.core.transformations

import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ArrayBuffer

/**
  * mapPartitionsWithIndex：可以拿到每个RDD中的分区id，以及分区中的数据
  */
object MapPartitionWithIndex {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setMaster("local")
      .setAppName("mapPartitionWithIndex")
    val sc = new SparkContext(conf)

    sc.textFile("./data/words",5)
      .mapPartitionsWithIndex((index, iter) => {
        val arr = ArrayBuffer[String]()
        iter.foreach(one=>{
          arr.append(s"partition = [$index], value = $one")
        })
        arr.iterator
      }, preservesPartitioning = true)
      .foreach(println)

    sc.stop()
  }
}
