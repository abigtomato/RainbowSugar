package com.abigtomato.spark.scala.sql.dataframe

import java.sql.Date

import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

/**
  * 统计pvuv
  * 将tuple类型的DataSet转换成DataFrme
  * 1.在创建Dataset之前 导入隐式转换 ：import spark.implicits._
  * 2.tuple最多支持22个元素
  * 3.Date 日期只支持java.sql.Date，支持的类型所有基本数据类型【见官网】
  */
object DataFrameFromTupleData {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local")
      .appName("DataFrameFromTupleData")
      .config("spark.sql.shuffle.partitions", 1)
      .getOrCreate()
    spark.sparkContext.setLogLevel("Error")

    val lines: Dataset[String] = spark.read.textFile("./data/pvuvdata")

    import spark.implicits._
//    implicit val tpencoder = org.apache.spark.sql.Encoders.kryo[(String,String,java.sql.Date,Long,Long,String,String)]
    val tupleData = lines.map(one => {
      val arr = one.split("\t")
      val date: Date = new Date(System.currentTimeMillis())
      (arr(0), arr(1),date, arr(3).toLong, arr(4).toLong, arr(5),arr(6))
    })

    val frame: DataFrame = tupleData.toDF("ip", "local", "date", "timestamp", "uid", "site", "action")
    frame.show(10)
    frame.printSchema()
    frame.createOrReplaceTempView("pvuv")

    // pv
    spark.sql("select site,count(site) as pv from pvuv where date = '2019-05-31' group by site order by pv desc").show()
    // uv
    spark.sql("select site ,count(distinct site,uid) as uv from pvuv where date = '2019-05-31' group by site order by uv desc").show()

    spark.close()
  }
}
