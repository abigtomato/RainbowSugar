package com.abigtomato.spark.scala.sql.examples

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

/**
  * 本案例是读取csv格式数据 ，找出当前行和下一行相比 ，某个字段变化的数据条目，例如：
  *   数据格式如下：
  *         ID,CHANGE,NAME
  *         id1,1,a
  *         id1,1,b
  *         id1,1,c
  *         id1,2,d
  *         id1,2,e
  *         id1,1,f
  *         id2,2,g
  *         id2,2,h
  *         id2,1,i
  *         id2,1,j
  *         id2,2,k
  *         id3,1,l
  *         id3,1,m
  *         id3,2,n
  *         id3,3,o
  *         id3,4,p
  *   目标：在每个ID组内，找出上一行和下一行相比change字段改变的行。
  *     找出的结果如下：
  *         id1,1,c
  *         id1,2,e
  *         id2,2,h
  *         id2,1,j
  *         id3,1,m
  *         id3,2,n
  *         id3,3,o
  *
  */
object FindChangeInfoDemo {
  def main(args: Array[String]): Unit = {

    /**
      * 读取 data/csvdata/test.csv数据测试代码
      */
    val spark: SparkSession =
      SparkSession.builder().appName("test")
        .master("local")
        .config("spark.sql.shuffle.partitions",1)
        .getOrCreate()
    spark.sparkContext.setLogLevel("Error")
    val df: DataFrame = spark.read.option("header",true).csv("./data/csvdata/*.csv")
    df.select("ID","CHANGE","NAME").show(false)
    df.createOrReplaceTempView("mytable")
    spark.sql(
      """
        |select
        |   id,change,name,row_number() over (partition by id order by name) as rank
        |from
        |   mytable
      """.stripMargin).createOrReplaceTempView("temp")


    spark.sql("select * from temp ").show()
    spark.sql(
      """
        |select
        |   t1.id,t1.change,t1.name
        |from temp t1,temp t2
        | where t1.id = t2.id and t1.rank = t2.rank-1 and t1.change != t2.change
      """.stripMargin).show()

    //或者以下使用join的写法也可以，将结果保存到csv文件中
    val df1 = spark.sql(
      """
        |select t1.id,t1.change,t1.name
        |from temp t1 join temp t2 on t1.rank = t2.rank-1
        |where t1.id = t2.id and t1.change != t2.change
      """.stripMargin).write.mode(SaveMode.Overwrite).csv("./tempresult/csvresult")


    /**
      * 读取data/csvdata/目录中所有csv数据
      *
      * 获取 3201 这个字段变化的行
      */
//    val spark = SparkSession.builder().master("local").appName("test").config("spark.sql.shuffle.partitions",1)getOrCreate()
//    spark.sparkContext.setLogLevel("Error")
//    val df = spark.read.option("header",true).csv("./data/csvdata/*/*.csv")
//    df.show(false)
//    df.createOrReplaceTempView("temp")
//    spark.sql(
//    """
//        | select row_number() over(partition by VID order by 1 ) as rank ,* from temp
//      """.stripMargin).createOrReplaceTempView("temp1")
//
//    spark.sql("select t1.`2201`,t1.* from temp1 t1 ,temp1 t2 where t1.vid = t2.vid and t1.rank = t2.rank-1 and t1.`2201` != t2.`2201` ").show(false)
//    spark.sql(
//      """
//        | select
//        |   t1.*
//        | from
//        |   temp1 t1 ,temp1 t2
//        | where
//        |   t1.vid = t2.vid and t1.rank = t2.rank-1 and t1.`2201` != t2.`2201`
//      """.stripMargin).write.mode(SaveMode.Overwrite).option("header",true).csv("./tempresult/csvresult1")

  }

}
