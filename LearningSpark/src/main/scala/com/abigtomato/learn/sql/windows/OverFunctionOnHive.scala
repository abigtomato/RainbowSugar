package com.abigtomato.spark.scala.sql.windows

import org.apache.spark.sql.{SaveMode, SparkSession}

/**
  * over 窗口函数
  * row_number() over(partition by xx order by xx) as rank
  * rank 在每个分组内从1开始
  */
object OverFunctionOnHive {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("over").enableHiveSupport().getOrCreate()
    spark.sql("use spark")
    spark.sql("create table if not exists sales (riqi string,leibie string,jine Int) " +
      "row format delimited fields terminated by '\t'")
    spark.sql("load data local inpath '/root/test/sales' into table sales")

    /**
      * rank 在每个组内从1开始
      *   5 A 200   --- 1
      *   3 A 100   ---2
      *   4 A 80   ---3
      *   7 A 60   ---4
      *
      *   1 B 100   ---1
      *   8 B 90  ---2
      *   6 B 80  ---3
      *   1 B 70  ---4
      */
    val result = spark.sql(
      "select"
                +" riqi,leibie,jine "
              + "from ("
                  + "select "
                      +"riqi,leibie,jine,row_number() over (partition by leibie order by jine desc) rank "
                  + "from sales) t "
              + "where t.rank<=3")
    result.write.mode(SaveMode.Append).saveAsTable("salesResult")
    result.show(100)
  }
}
