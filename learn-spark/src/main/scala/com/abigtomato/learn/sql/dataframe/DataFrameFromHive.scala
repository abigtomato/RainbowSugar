package com.abigtomato.spark.scala.sql.dataframe

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

/**
 * 读取Hive中的数据创建dataframe
 */
object DataFrameFromHive {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("CreateDataFrameFromHive")
      .enableHiveSupport()
      .getOrCreate()

    spark.sql("use spark")
    spark.sql("drop table if exists student_infos")
    spark.sql("create table if not exists student_infos (name string,age int) row format  delimited fields terminated by '\t'")
    spark.sql("load data local inpath '/root/test/student_infos' into table student_infos")

    spark.sql("drop table if exists student_scores")
    spark.sql("create table if not exists student_scores (name string,score int) row format delimited fields terminated by '\t'")
    spark.sql("load data local inpath '/root/test/student_scores' into table student_scores")

    val frame: DataFrame = spark.table("student_infos")
    frame.show(100)

    val df = spark.sql("select si.name,si.age,ss.score from student_infos si,student_scores ss where si.name = ss.name")
    df.show(100)
    spark.sql("drop table if exists good_student_infos")

    // 将结果写入到hive表中
    df.write.mode(SaveMode.Overwrite).saveAsTable("good_student_infos")
  }
}
