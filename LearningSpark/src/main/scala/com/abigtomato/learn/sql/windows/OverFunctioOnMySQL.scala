package com.abigtomato.spark.scala.sql.windows

import java.util.Properties

import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * 在mysql中使用开窗函数
  * row_number() over(partition by xx order by xx) as rank
  * rank 在每个分组内从1开始
  * 需要创建表 sales:
  *   create table sales (riqi VARCHAR(20),leibie VARCHAR(20),jine VARCHAR(20))
  * 使用Execl 可以导入数据，复制粘贴即可。
  *
  */
object OverFunctioOnMySQL {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local").appName("overfunctioonmysql").getOrCreate()
    val properties = new Properties()
    properties.setProperty("user", "root")
    properties.setProperty("password", "123456")

    val sql = "select  riqi,leibie,jine from " +
      "(select riqi,leibie,jine, row_number() over (partition by leibie order by jine desc) as 'rank'  " +
      "from sales) t  where t.rank<=3"
    val person: DataFrame = spark.read.jdbc("jdbc:mysql://192.168.179.101:3306/spark",s"($sql) T",properties)
    person.show()
  }
}
