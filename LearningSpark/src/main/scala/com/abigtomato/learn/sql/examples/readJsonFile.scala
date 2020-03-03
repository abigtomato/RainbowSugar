package com.abigtomato.spark.scala.sql.examples

import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.{DataFrame, SparkSession}

object readJsonFile {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("readJsonFile")
      .master("local")
      .getOrCreate()

    import org.apache.spark.sql.functions._
    import spark.implicits._
    /**
      * get_json_object() 方法不识别单引号引起来的json格式字符串
      */
    //    val list = List[String](
    //      "{'name':'zhangsan','age':18}",
    //      "{'name':'lisi','age':19}",
    //      "{'name':'wangwu','age':20}"
    //    )

    val list = List[String](
      "{\"name\":\"zhangsan\",\"age\":20}",
      "{\"name\":\"lisi\",\"age\":21}",
      "{\"name\":\"wangwu\",\"age\":22}",
      "{\"name\":\"zhaoliu\",\"age\":23}"
    )


    val frame: DataFrame = list.toDF("infos")
//    val frame = spark.read.textFile("./data/json").toDF("infos")
    frame.show(100,false)
    frame.printSchema()

    //使用get_json_object() 可以获取其中某些列组成新的 DataFrame，注意：get_json_object() 方法不识别单引号引起来的json格式字符串
    val result: DataFrame = frame.select(get_json_object($"infos","$.name").as("name"),get_json_object($"infos", "$.age").cast(IntegerType).as("age"))
//    val result: DataFrame = frame.select(get_json_object($"infos","$.name").as("name"),get_json_object($"infos", "$.age").cast(IntegerType).as("age"))
    result.show(100)
    result.printSchema()

  }
}
