package com.abigtomato.learn.sql.examples.log

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{count, row_number, sum}
import org.apache.spark.sql.{DataFrame, SparkSession}

object TopNStatJob {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .config("spark.sql.sources.partitionColumnTypeInference.enabled", "false")
      .appName("TopNStatJob")
      .master("local").getOrCreate()

    val accessDF: DataFrame = spark.read.format("parquet").load("src/main/resources/data/log/clean")

    accessDF.printSchema()
    accessDF.show(false)

    this.videoAccessTopNStat(spark, accessDF)
    this.cityAccessTopNStat(spark, accessDF)
    this.videoTrafficsTopNStat(spark, accessDF)

    spark.close()
  }

  /**
   * 统计每天每个视频的流量总计
   * @param spark
   * @param accessDF
   */
  def videoTrafficsTopNStat(spark: SparkSession, accessDF: DataFrame): Unit = {
    import spark.implicits._

    accessDF
      .filter($"day" === "20200216" && $"cmsType" === "video")
      .groupBy("day", "cmsId")
      .agg(sum("traffic").as("traffics"))
      .orderBy($"traffics".desc)
      .show(false)
  }

  /**
   * 统计每天每个城市下的topN视频
   * @param spark
   * @param accessDF
   */
  def cityAccessTopNStat(spark: SparkSession, accessDF: DataFrame): Unit = {
    import spark.implicits._

    val cityAccessTopNDF: DataFrame = accessDF
      .filter($"day" === "20170511" && $"cmsType" === "video")
      .groupBy("day", "city", "cmsId")
      .agg(count("cmsId").as("times"))
    cityAccessTopNDF.show(false)

    cityAccessTopNDF.select(
      cityAccessTopNDF("day"),
      cityAccessTopNDF("city"),
      cityAccessTopNDF("cmsId"),
      cityAccessTopNDF("times"),
      row_number()
        .over(Window.partitionBy(cityAccessTopNDF("city")).orderBy(cityAccessTopNDF("times").desc))
        .as("times_rank")
    ).filter("times_rank <= 3").show(false)
  }

  /**
   * 统计每天最受欢迎的topN课程
   * @param spark
   * @param accessDF
   */
  def videoAccessTopNStat(spark: SparkSession, accessDF: DataFrame): Unit = {
    import spark.implicits._

    accessDF
      .filter($"day" === "20170511" && $"cmsType" === "video")
      .groupBy("day", "cmsId")
      .agg(count("cmsId").as("times"))
      .orderBy($"times".desc)
      .show(false)

    accessDF.createOrReplaceTempView("access_log")
    spark.sql("select day, cmsId, count(1) as times from access_log " +
      "where day = '20170511' and cmsType = 'video' " +
      "group by day, cmsId " +
      "order by times desc").show(false)
  }
}
