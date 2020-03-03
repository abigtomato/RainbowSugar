package com.abigtomato.spark.scala.mllib.datatype

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

object RandomSplitTest {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setMaster("local")
      .setAppName("RandomSplitTest")
    val sc = new SparkContext(conf)
    val rdd = sc.makeRDD(List(1, 2, 3, 4, 5, 6, 7, 8, 9, 10))
    /**
     * 数组的长度决定了将这个RDD分割成多少个RDD
     * 数组中每一个元素的值决定了每一个RDD中的数据量
     * randomSplit 是action类算子吗？ 不是    是transformation类算子
     */
    val rddArr:Array[RDD[Int]] = rdd.randomSplit(Array(0.2, 0.8))
    rddArr.foreach { rdd => println(rdd.count) }
    //cartesian
    
    val rdd1 = sc.makeRDD(List(1,2,3,4))
    val rdd2 = sc.makeRDD(List(2,3,4,5))
    val cartesianRDD = rdd1.cartesian(rdd2)
    cartesianRDD.foreach(println)
    
    
    //zip 拉链 拉锁    zip算子能够将两个RDD横向合并在一起
    rdd1
      .zip(rdd2)
      .foreach(println)
    sc.stop()
  }
}