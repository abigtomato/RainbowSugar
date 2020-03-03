package com.abigtomato.spark.scala.sql.examples

import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * 读取嵌套的json格式文件
  *
  *   json格式如下：
  *     {"name":"wangwu","score":80,"infos":{"age":23,"gender":'man'}}
  *
  *     对于读取嵌套的josn格式的数据，可以直接infos.列名称来获取值
  */
object readNestJsonFile {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder().master("local").appName("readNestJsonFile").getOrCreate()
    //读取嵌套的json文件
    val frame: DataFrame = spark.read.format("json").load("./data/NestJsonFile")
    frame.printSchema()
    frame.show(100)
    frame.createOrReplaceTempView("infosView")
    spark.sql("select name,infos.age,score,infos.gender from infosView").show(100)
  }
}
