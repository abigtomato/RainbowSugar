package com.abigtomato.spark.scala.sql.dataframe

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

/**
 * 通过parquet加载dataframe
 */
object DataFrameFromParquet {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local")
      .appName("DataFrameFromParquet")
      .getOrCreate()

    val df1: DataFrame = spark.read.format("json").load("./data/json")
    df1.show()

    /**
     * 保存成parquet文件
     *  - Append: 如果文件存在就追加
     *  - Overwrite: 覆盖写
     *  - Ignore: 忽略
     *  - ErrorIfExists: 报错
     */
    df1.write.mode(SaveMode.Append).format("parquet").save("./data/parquet")

    /**
     * 读取parquet文件
     */
    val df2: DataFrame = spark.read.parquet("./data/parquet")
    val count: Long = df2.count()
    println(count)
    df2.show(100)

    spark.close()
  }
}
