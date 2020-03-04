package com.abigtomato.learn.sql.udaf

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.expressions.{Aggregator, MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Dataset, Encoder, Encoders, Row, SparkSession, TypedColumn}

/**
 * UDAF用户自定义聚合函数
 */
object UDAF {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local")
      .appName("UDAF")
      .getOrCreate()
    import spark.implicits._

    val rdd: RDD[(Int, String, Int)] = spark.sparkContext
      .makeRDD(List((1, "zhangsan", 20), (2, "lisi", 30), (3, "wangwu", 40)))

    val peopleRDD: RDD[People] = rdd.map {
      case (id, name, age) => People(id, name, age)
    }
    val peopleDS: Dataset[People] = peopleRDD.toDS()

    /**
     * 弱类型UDAF
     */
    peopleDS.createOrReplaceTempView("people")
    // 注册UDAF函数
    spark.udf.register("AVGAGE", new MyAgeAvgFuntion())
    spark.sql("select name, AVGAGE(age) as avg from people").show()

    /**
     * 强类型UDAF
     */
    val udaf = new MyAgeAvgClassFunction
    // 将聚合函数转换为查询列
    val avgCol: TypedColumn[People, Double] = udaf.toColumn.name("avgAge")
    peopleDS.select(avgCol).show()

    spark.close()
  }
}

/**
 * 声明用户自定义聚合函数（弱类型）
 */
class MyAgeAvgFuntion extends UserDefinedAggregateFunction {

  // 输入数据的类型
  def inputSchema: StructType = {
    new StructType().add("age", LongType)
  }

  // 缓冲区初始化
  def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer(0) = 0L
    buffer(1) = 0L
  }

  // 每个组，有新的值进来的时候，进行分组对应的聚合值的计算
  def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    buffer(0) = buffer.getLong(0) + input.getLong(0)
    buffer(1) = buffer.getLong(1) + 1
  }

  // 多个节点的缓冲区合并操作
  def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    // sum
    buffer1(0) = buffer1.getLong(0) + buffer2.getLong(0)
    // count
    buffer1(1) = buffer1.getLong(1) + buffer2.getLong(1)
  }

  // 聚合操作时，所处理的数据的类型
  def bufferSchema: StructType = {
    new StructType().add("sum", LongType).add("count", LongType)
  }

  // 返回一个最终的聚合值，要和dataType的类型一一对应
  def evaluate(buffer: Row): Any = {
    buffer.getLong(0).toDouble / buffer.getLong(1)
  }

  // 最终函数返回值的类型
  def dataType: DataType = DataTypes.DoubleType

  // 多次运行相同的输入总是相同的输出，确保函数的稳定性
  def deterministic: Boolean = true
}

case class People(id: Int, name: String, age: Int)
case class AvgBuffer(var sum: Int, var count: Int)

/**
 * 声明用户自定义聚合函数（强类型）
 */
class MyAgeAvgClassFunction extends Aggregator[People, AvgBuffer, Double] {

  // 缓冲区初始化
  override def zero: AvgBuffer = AvgBuffer(0, 0)

  // 数据聚合操作
  override def reduce(b: AvgBuffer, a: People): AvgBuffer = {
    b.sum = b.sum + a.age
    b.count = b.count + 1
    b
  }

  // 缓冲区的合并操作
  override def merge(b1: AvgBuffer, b2: AvgBuffer): AvgBuffer = {
    b1.sum = b1.sum + b2.sum
    b1.count = b1.count + b2.count
    b1
  }

  // 最后的计算操作
  override def finish(reduction: AvgBuffer): Double = {
    reduction.sum.toDouble / reduction.count
  }

  // 缓冲区编码
  override def bufferEncoder: Encoder[AvgBuffer] = Encoders.product

  // 输出数据编码
  override def outputEncoder: Encoder[Double] = Encoders.scalaDouble
}
