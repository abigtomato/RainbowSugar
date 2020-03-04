package com.abigtomato.spark.scala.sql.dataframe

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

/**
  * 读取json格式的文件加载DataFrame
  * 注意：
  *   - 读取json格式的两种方式
  *   - df.show默认显示前20行，使用df.show(行数)显示多行
  *   - dataFrame加载过来会按照列的ascii码排序
  *   - df.printSchema显示列的Schema信息
  *   - 创建临时表的两种方式和区别：createOrReplaceTempView  | createGlobalTempView  global_temp.xxx
  */
object DataFrameFromJsonFile {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder()
      .appName("DataFrameFromJsonFile")
      .master("local")
      .getOrCreate()

    // 需要导入隐式转换，下面的$ toDF都会用到隐式转换
    import spark.implicits._

//    val df: DataFrame = spark.read.json("./data/json")
    val df: DataFrame= spark.read.format("json").load("./data/json")
    df.show()
    // 显示DataFrame，默认显示前20行（参数指定显示的行数）
    df.show(100)
    // 打印schema信息
    df.printSchema()

    /**
     * 获取值
     */
    val rows1: Array[Row] = df.take(100)
    val row2: Row = df.first()
    val rows3: Array[Row] = df.head(4)
    println(rows1.toBuffer)
    println(row2)
    println(rows3.toBuffer)

    /**
     * 使用DataFrame原生api
     */
    df.select(df.col("name")).show()
    df.select("name").show()
    df.select("name", "age").show()
    df.filter($"age" > 18).show()  // 查找年龄大于18的人
    df.filter(df.col("name").equalTo("zhangsan")).show()
    df.filter("name='zhangsan4' or name = 'zhangsan5'").show() // 可以写表达式
    df.sort($"age".asc,$"name".desc).show()  // 按照age升序，按照name降序排列
    df.sort(df.col("age").asc, df.col("name").desc).show()  // 按照age升序，按照name降序排列
    // 给定别名
    df.select(df.col("name")
      .as("studentName"), df.col("age")
      .alias("studentAge"))
      .show()
    df.select($"name".alias("studentName"), $"age").show()
    df.select($"name",($"age"+1).as("addAge")).show() // age+1之后，直接列名 成为了 age+1
    df.groupBy("age").count().show()  // 按照age分组

    /**
     * 使用sql操作
     *  - createOrReplaceTempView:创建临时的视图
     *  - createTempView
     *  - createGlobalTempView:创建全局的视图，访问全局的视图使用  global_temp.表名,全局的表可以跨spark session访问
     *  - createOrReplaceGlobalTempView
     */
    df.createOrReplaceTempView("students")
    df.createGlobalTempView("students")
    val frame: DataFrame = spark.sql("select  * from students")
    frame.show()

    import spark._
    sql("select * from students where age > 18 and name = 'zhangsan5'").show(100)
    sql("select * from global_temp.students where name like 'wang%'").show(100)

    // 创建一个新的session
    spark.newSession().sql("SELECT * FROM global_temp.students").show()
    spark.newSession().sql("SELECT * FROM students").show()

    /**
     * 将DataFrame转换成RDD
     */
    val rdd: RDD[Row] = df.rdd
    rdd.foreach(row => {
      // 打印row
      println(row)

      /**
       * row中获取值,两种方式 1. getAs("字段名") 2.getAs(下标)
       */
      val name = row.getAs[String]("name")
      val age = row.getAs[Long]("age")

//      val name = row.getAs[String](1)
//      val age = row.getAs[Long](0)
      println("name = " + name + ", age = " + age)
    })

    spark.close()
  }
}
