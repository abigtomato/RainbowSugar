package com.abigtomato.learn.sql.udf

import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * UDF用户自定义函数
 */
object UDF {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local")
      .appName("UDF")
      .getOrCreate()
    import spark.implicits._

    val nameList: List[String] = List[String]("zhangsan", "lisi", "wangwu", "zhaoliu", "tianqi")
    val nameDF: DataFrame = nameList.toDF("name")
    nameDF.createOrReplaceTempView("students")
    nameDF.show()

    spark.udf.register("STRLEN1", (name: String) => name.length)
    spark.sql("select name, STRLEN1(name) as length from students order by length desc").show(100)

    spark.udf.register("STRLEN2", (name: String, i: Int) => name.length + i)
    spark.sql("select name, STRLEN2(name, 10) as length from students order by length desc").show(100)

    spark.close()
  }
}
