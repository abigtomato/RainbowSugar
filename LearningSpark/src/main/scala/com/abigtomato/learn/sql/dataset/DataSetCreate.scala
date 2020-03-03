package com.abigtomato.spark.scala.sql.dataset

import org.apache.spark.sql.{Dataset, SparkSession}

/**
 * 创建DataSet
 */
object DataSetCreate {

  case class Student(name: String, age: Long)
  case class Person(id: Int, name: String, age: Int, score: Double)

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local")
      .appName("CreateStructDataSet")
      .getOrCreate()
    import spark.implicits._

    /**
     * 1.由集合创建DataSet
     */
    // 直接映射成Person类型的DataSet
    val personDs: Dataset[Person] = List[Person](
      Person(1,"zhangsan", 18, 100),
      Person(2,"lisi", 19, 200),
      Person(3,"wangwu", 20, 300)).toDS()
    personDs.show(100)

    /**
     * 2.由json文件和类直接映射成DataSet
     */
    val lines: Dataset[Student] = spark.read.json("src/main/resources/data/json").as[Student]
    lines.show()

    /**
     * 3.读取外部文件直接加载DataSet
     */
    val dataSet: Dataset[String] = spark.read.textFile("src/main/resources/data/people.txt")
    val result: Dataset[Person] = dataSet.map(line => {
      val arr: Array[String] = line.split(",")
      Person(arr(0).toInt, arr(1).toString, arr(2).toInt, arr(3).toDouble)
    })
    result.show()

    spark.close()
  }
}
