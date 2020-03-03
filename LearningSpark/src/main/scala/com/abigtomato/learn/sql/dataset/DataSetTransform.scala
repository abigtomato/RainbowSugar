package com.abigtomato.spark.scala.sql.dataset

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

/**
 * RDD，DataFrame，DataSet相互转换
 */
object DataSetTransform {

  case class People(id: Int, name: String, age: Int)

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local")
      .appName("CreateStructDataSet")
      .getOrCreate()

    // 隐式转换规则，这里的spark不是包名的含义，是SparkSession对象的名字
    import spark.implicits._

    // 创建RDD
    val rdd: RDD[(Int, String, Int)] = spark.sparkContext
      .makeRDD(List((1, "zhangsan", 20), (2, "lisi", 30), (3, "wangwu", 40)))

    // RDD转换为DF
    val df: DataFrame = rdd.toDF("id", "name", "age") // 设置数据结构

    // DF转换为DS
    val ds: Dataset[People] = df.as[People] // 指定类型约束

    // DS转换为DF
    val df1: DataFrame = ds.toDF()

    // DF转换为RDD
    val rdd1: RDD[Row] = df1.rdd
    rdd1.foreach(row => println(row.getString(1)))  // 通过索引访问

    // RDD转换为DS
    val peopleRDD: RDD[People] = rdd.map { case (id, name, age) => People(id, name, age) }  // 映射成拥有结构和类型的RDD
    val peopleDS: Dataset[People] = peopleRDD.toDS()

    // DS转换RDD
    val rdd2: RDD[People] = peopleDS.rdd
    rdd2.foreach(println)

    spark.close()
  }
}
