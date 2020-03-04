package com.abigtomato.spark.scala.sql.dataframe

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

/**
 * 通过动态创建Schema的方式加载DataFrame
 * 注意：动态创建的ROW中数据的顺序要与创建Schema的顺序一致。
 */
object DataFrameFromRDDWithSchema {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local")
      .appName("createdataframefromrddwithschema")
      .getOrCreate()
    import spark.implicits._

    val peopleRDD: RDD[String] = spark.sparkContext.textFile("./data/people.txt")

    /**
     * 将peopleRDD转换成RDD[Row]
     */
    val rowRDD: RDD[Row] = peopleRDD.map(one => {
      val arr: Array[String] = one.split(",")
      Row(arr(0).toInt, arr(1), arr(2).toInt, arr(3).toLong)
    })

    val structType: StructType = StructType(List[StructField](
      StructField("id", IntegerType, nullable = true),
      StructField("name", StringType, nullable = true),
      StructField("age", IntegerType, nullable = true),
      StructField("score", LongType, nullable = true)
    ))

    val frame: DataFrame = spark.createDataFrame(rowRDD, structType)
    frame.show()
    frame.printSchema()
    frame.createOrReplaceTempView("people")

//    val schemaString = "id name age score"
//    /**
//      * 动态创建Schema方式
//      */
//    val fields: Array[StructField] = schemaString.split(" ").map(fieldName => StructField(fieldName, StringType, nullable = true))
//    val schema: StructType = StructType(fields)
//
//    val rowRDD: RDD[Row] = peopleRDD
//      .map(_.split(","))
//      .map(attributes => Row(attributes(0).trim, attributes(1).trim, attributes(2).trim, attributes(3).trim))
//
//    // 创建DataFrame
//    val peopleDF: DataFrame = spark.createDataFrame(rowRDD, schema)
//    peopleDF.show()
//    peopleDF.printSchema()
//
//    //注册临时表
//    peopleDF.createOrReplaceTempView("people")

    val results: DataFrame = spark.sql("SELECT name FROM people")
    val value: Dataset[String] = results.map(attributes => "Name: " + attributes(0).toString)
    value.show()
    value.withColumnRenamed("value","myCol").show()

    spark.close()
  }
}
