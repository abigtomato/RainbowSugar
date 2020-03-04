package com.abigtomato.spark.scala.sql.examples

import org.apache.spark.sql._
import org.apache.spark.sql.expressions.{Window, WindowSpec}

/**
  *  userAccInfo.txt ：【注意，这个表中的数据相同用户相同日期中不能有同一个用户信息，这里的文件中333用户06-20故意设置重复数据了】
  *   用户ID  日期  访问时长
  * +---+----------+--------+
  * |uid|      date|duration|
  * +---+----------+--------+
  * |111|2019-06-20|       1|
  * |111|2019-06-21|       2|
  * |111|2019-06-22|       3|
  * |222|2019-06-20|       4|
  * |222|2019-06-21|       5|
  * |222|2019-06-22|       6|
  * |333|2019-06-20|       7|
  * |333|2019-06-21|       8|
  * |333|2019-06-22|       9|
  * |333|2019-06-20|      10|
  * |444|2019-06-23|      11|
  * +---+----------+--------+
  *
  *  统计所有产品ID同一日期被访问的总时长
  */
case class ProductInfo(uid:String,date:String,duration:Long)

object AddUpInfo {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder().master("local").appName("text").getOrCreate()
    spark.sparkContext.setLogLevel("Error")
    import spark.implicits._
    val ds: Dataset[String] = spark.read.textFile("./data/userAccInfo.txt")

    val rowDs: Dataset[ProductInfo] = ds.map(line => {
      ProductInfo(line.split("\t")(0), line.split("\t")(1), line.split("\t")(2).toLong)
    })

    val frame: DataFrame = rowDs.toDF()

    frame.show(100)
    frame.createOrReplaceTempView("userAccTable")

    /**
      * ********************************************
      * 1.统计每个用户访问的历史累计时长,要求同一用户每天都累加之前天数的值，得到结果。
      * ********************************************
      */
    //使用开窗函数实现，注意：按照用户id分组时，相同天中访问时长会合并计算
    spark.sql(
      """
            select
              uid,date,sum(duration) over (partition by uid order by date asc) as sum_duration
            from userAccTable
          """).show
    //使用原生api实现
    import org.apache.spark.sql.functions._
    val spec: WindowSpec = Window.partitionBy("uid").orderBy($"date".asc)
    val result1: DataFrame = frame.select($"uid",$"date",sum("duration").over(spec).alias("sum_duration"))
    result1.show(100)

    /**
      * ********************************************
      * 2.统计每个用户访问的历史总计时长
      * ********************************************
      */
    //sql实现
    spark.sql(
      """
        |select
        |   uid ,date,sum(duration) over (partition by uid ) as sum_duration
        |from
        |   userAccTable
      """.stripMargin
    ).show(100)
    //使用api实现
    val spec1: WindowSpec = Window.partitionBy("uid")
    val result2: DataFrame = frame.select($"uid",$"date",sum("duration").over(spec1).alias("sum_duration"))
    result2.show(100)

    /**
      * ********************************************
      * 3.根据用户id 累加前1天用户访问的时长,这里就是累加前一条的数据，也可以累加前2条的数据
      *
      *   preceding：用于累加前N行（分区之内）。若是从分区第一行头开始，则为 unbounded。 N为：相对当前行向前的偏移量
      *   following ：与preceding相反，累加后N行（分区之内）。若是累加到该分区结束，则为 unbounded。N为：相对当前行向后的偏移量
      *   current row：顾名思义，当前行，偏移量为0
      *   注意：上边的前N，后M，以及current row均会累加该偏移量所在行
      * ********************************************
      */
    //sql实现
    spark.sql(
      """
        | select
        |   uid,date,sum(duration) over(partition by uid order by date asc rows between 1 preceding and current row) as sum_duration
        | from
        |   userAccTable
      """.stripMargin).show(100)

    //原生api实现
    val spec2: WindowSpec = Window.partitionBy("uid").orderBy("date").rowsBetween(-1,0)
    val result3 = frame.select($"uid",$"date",sum($"duration").over(spec2).alias("sum_duration"))
    result3.show(100)

    /**
      *
      */

  }
}













