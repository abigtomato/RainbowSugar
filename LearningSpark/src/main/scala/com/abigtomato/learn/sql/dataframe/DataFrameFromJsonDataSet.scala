package com.abigtomato.spark.scala.sql.dataframe

import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

/**
 * 通过json格式的dataset数据创建dataframe
 *  - 1.加载成的DataFrame会自动按照列的Ascii码排序
 *  - 2.自己写sql组成的DataFrame的列不会按照列的Ascii码排序
 */
object DataFrameFromJsonDataSet {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("createDFFromJsonRDD")
      .master("local")
      .getOrCreate()
    spark.sparkContext.setLogLevel("Error")

    val jsonList = List[String](
      "{\"name\":\"zhangsan\",\"age\":20}",
      "{\"name\":\"lisi\",\"age\":21}",
      "{\"name\":\"wangwu\",\"age\":22}")

    val jsonList2 = List[String](
      "{\"name\":\"zhangsan\",\"score\":100}",
      "{\"name\":\"lisi\",\"score\":200}",
      "{\"name\":\"wangwu\",\"score\":300}")

    import spark.implicits._

    val jsonDs: Dataset[String] = jsonList.toDS()
    val scoreDs: Dataset[String] = jsonList2.toDS()
    val df2: DataFrame = spark.read.json(jsonDs)
    val df3 :DataFrame = spark.read.json(scoreDs)
    df2.show()
    df3.show()

    df2.createOrReplaceTempView("person")
    df3.createOrReplaceTempView("score")

    val frame: DataFrame = spark.sql("select * from person")
    frame.show()

    spark.sql("select t1.name ,t1.age,t2.score from person  t1, score  t2 where t1.name = t2.name").show()
  }
}
