package com.abigtomato.rainbowsugar.learn.project.log

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

/**
 * 数据清洗
 */
object StatCleanJob {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("StatCleanJob").master("local").getOrCreate()

    val accessRDD: RDD[String] = spark.sparkContext.textFile("src/main/resources/data/log/access.log")

    val accessDF: DataFrame = spark.createDataFrame(accessRDD.map(log => AccessConvertUtil.parseLog(log)),
      AccessConvertUtil.struct)

    accessDF.show(false)
    accessDF.printSchema()

    accessDF.coalesce(1).write
      .format("parquet")
      .partitionBy("day")
      .mode(SaveMode.Overwrite)
      .save("src/main/resources/data/log/clean")

    spark.close()
  }
}
