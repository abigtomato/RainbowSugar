package com.abigtomato.spark.scala.sql.examples

import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * 读取嵌套的jsonArray 数组，格式：
  *   {"name":"zhangsan","age":18,"scores":[{"yuwen":98,"shuxue":90,"yingyu":100},{"dili":98,"shengwu":78,"huaxue":100}]}
  *
  *   explode 函数作用是的数组展开，数组中的每个json对象都是一条数据
  */
object readJsonArray {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder().master("local").appName("readJsonArray").getOrCreate()
    val frame: DataFrame = spark.read.json("./data/jsonArrayFile")
    //不折叠显示
    frame.show(false)
    frame.printSchema()
    import org.apache.spark.sql.functions._
    import spark.implicits._
    val transDF: DataFrame = frame.select($"name", $"age", explode($"scores")).toDF("name", "age","allScores")
    transDF.show(100,false)
    transDF.printSchema()
    val result: DataFrame = transDF.select($"name",$"age",
            $"allScores.yuwen" as "yuwen",
            $"allScores.shuxue" as "shuxue",
            $"allScores.yingyu" as "yingyu",
            $"allScores.dili" as "dili",
            $"allScores.shengwu" as "shengwu",
            $"allScores.huaxue" as "huaxue"
    )
    result.show(100)
  }
}
