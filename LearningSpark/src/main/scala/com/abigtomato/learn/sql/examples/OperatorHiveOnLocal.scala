package com.abigtomato.spark.scala.sql.examples

import org.apache.spark.sql.{SaveMode, SparkSession}

/**
  * 本地读取Hive中的数据，必须将configFiles中的 core-site.xml 和 hdfs-site.xml放在resources目录中
  * 运行完之后要在resources目录中将这两个文件删除，不然其他运行都会基于HDFS模式运行
  *     做了调节：
  *     1.File | Settings | Build, Execution, Deployment | Compiler 中 VM options :-Xmx512M
  *     2.运行程序时：-server -Xms512M -Xmx1024M -XX:PermSize=256M -XX:MaxNewSize=512M -XX:MaxPermSize=512M
  */
object OperatorHiveOnLocal {
  def main(args: Array[String]): Unit = {
      val spark = SparkSession.builder().appName("CreateDataFrameFromHive").config("hive.metastore.uris","thrift://mynode1:9083").master("local").enableHiveSupport().getOrCreate()
      spark.sql("use spark")
      spark.sql("drop table if exists student_infos")
      spark.sql("create table if not exists student_infos (name string,age int) row format  delimited fields terminated by '\t'")
      spark.sql("load data local inpath './data/student_infos' into table student_infos")

      spark.sql("drop table if exists student_scores")
      spark.sql("create table if not exists student_scores (name string,score int) row format delimited fields terminated by '\t'")
      spark.sql("load data local inpath './data/student_scores' into table student_scores")

      val df = spark.sql("select si.name,si.age,ss.score from student_infos si,student_scores ss where si.name = ss.name")
      spark.sql("drop table if exists good_student_infos")

      /**
        * 将结果写入到hive表中
        */
      df.write.mode(SaveMode.Overwrite).saveAsTable("good_student_infos")
  }
}
