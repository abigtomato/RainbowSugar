package com.abigtomato.spark.scala.sql.dataframe

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Encoder, Row, SparkSession}

/**
 * 通过反射的方式将DataSet(RDD)转换成DataFrame
 * 步骤：
 *  - 1.将文件读成RDD或者DataSet转换成 某种对象类型[Person]的 DataSet[Person]或者RDD[Person]
 *  - 2.直接使用 DataSet[Person].toDF  或者RDD[Person].toDF
 * 注意：自动生成的DataFrame 会按照对象中的属性顺序显示
 */
object DataFrameFromRDDWithReflection {

  case class Person(i: Int, str: String, i1: Int, d: Double)

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local")
      .appName("DataFrameFromRDDWithReflection")
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    import spark.implicits._

    /**
     * 直接读取文件为DataSet
     */
    val person: Dataset[String] = spark.read.textFile("./data/people.txt")
    val personDS: Dataset[Person] = person.map(one => {
      val arr = one.split(",")
      Person(arr(0).toInt, arr(1).toString, arr(2).toInt, arr(3).toDouble)
    })

    /**
     * 直接读取文件为RDD
     */
    val rdd: RDD[String] = spark.sparkContext.textFile("./data/people.txt")
    val personRDD: RDD[Person] = rdd.map(one => {
      val arr = one.split(",")
      Person(arr(0).toInt, arr(1), arr(2).toInt, arr(3).toDouble)
    })

    val frame: DataFrame = personRDD.toDF()
    frame.show()

    /**
     * dataFrame api 操作
     */
    frame.createOrReplaceTempView("people")
    val teenagersDF: DataFrame = spark.sql("SELECT name, age FROM people WHERE age BETWEEN 13 AND 19")
    teenagersDF.show()
    // 根据row中的下标获取值
    teenagersDF.map((teenager: Row) => "Name: " + teenager(0)).show()
    // 根据row中的字段获取值
    teenagersDF.map(teenager => "Name: " + teenager.getAs[String]("name")).show()

    /**
     * 数据集[Map[K,V]没有预定义的编码器，在这里定义
     */
    implicit val mapEncoder: Encoder[Map[String, Any]] = org.apache.spark.sql.Encoders.kryo[Map[String, Any]]
    // Map 没有额外的编码器，在转换过程中Map 需要隐式转换的编码器
    val result: Dataset[Map[String, Any]] = teenagersDF.map(teenager => teenager.getValuesMap[Any](List("name", "age")))
    result.collect().foreach(println)

    spark.close()
  }
}
