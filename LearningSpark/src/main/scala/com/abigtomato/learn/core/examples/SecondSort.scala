package com.abigtomato.spark.scala.core.examples

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * 二次排序问题
 */
object SecondSort {
  case class SecondSortKey(first: Int, second: Int) extends Ordered[SecondSortKey] {
    def compare(that: SecondSortKey): Int = {
      if (this.first - that.first == 0)
        this.second - that.second
      else
        this.first - that.first
    }
  }

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("secondarySort")
      .setMaster("local")
    val sc = new SparkContext(conf)

    val lines = sc.textFile("./data/secondSort.txt")
    val transRDD: RDD[(SecondSortKey, String)] = lines
      .map(s => (SecondSortKey(s.split(" ")(0).toInt, s.split(" ")(1).toInt), s))
    transRDD.sortByKey(ascending = false).map(_._2).foreach(println)

    sc.stop()
  }
}
