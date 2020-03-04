package com.albert.api.transform

import com.albert.api.source.SensorReading
import org.apache.flink.api.common.functions.{FilterFunction, RichFilterFunction, RichFlatMapFunction, RichFunction}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

object LearnTransform {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val fileStream = env.readTextFile("D:\\WorkSpace\\javaproject\\learn-flink\\src\\main\\resources\\sensor.csv")

    // 1.基本转换和简单聚合算子
    val dataStream: DataStream[SensorReading] = fileStream.map(data => {
      val dataArr = data.split(",")
      SensorReading(dataArr(0).trim, dataArr(1).trim.toLong, dataArr(2).trim.toDouble)
    }).keyBy(_.id)
//      .sum("temperature")
      // 输出当前传感器最新的温度+10，时间戳是上一次聚合结果的时间戳+1
      .reduce((x, y) => SensorReading(x.id, x.timestamp + 1, y.temperature + 10))
//    dataStream.print("fileStream")

    // 2.多流转换算子
    val splitStream = dataStream.split(data => {
      // 传感器数据按照温度高低（以 30 度为界），拆分成两个流
      if (data.temperature > 30) Seq("high") else Seq("low")
    })
    val highStream = splitStream.select("high")
    val lowStream = splitStream.select("low")
    val allStream = splitStream.select("high", "low")
//    highStream.print("highStream")
//    lowStream.print("lowStream")
//    allStream.print("allStream")

    // 3.合并两条流
    val warningStream = highStream.map(data => (data.id, data.temperature))
    val connectStream = warningStream.connect(lowStream)
    val coMapStream = connectStream.map(
      // 合并后分别对两条流进行不同操作
      warningData => (warningData._1, warningData._2, "warning"),
      lowData => (lowData.id, "healthy")
    )
    coMapStream.connect(allStream)  // coMap操作之后可继续进行connect操作
//    coMapStream.print("coMapStream")

    // 4.合并多条流
    val unionStream = highStream.union(lowStream, allStream)
//    unionStream.print("unionStream")

    // 5.自定义UDF函数
    dataStream.filter(new FilterFunction[SensorReading] {
      // 匿名类形式
      override def filter(t: SensorReading): Boolean = {
        t.id.startsWith("sensor_1")
      }
    }).print("myFilter")
//    dataStream.filter(_.id.startsWith("sensor_1")).print("myFilter")  // 匿名函数形式

    // 富函数形式
    dataStream.flatMap(new RichFlatMapFunction[SensorReading, (Int, String, Long, Double)] {
      var subtask = 0

      override def open(parameters: Configuration): Unit = {
        // 这里可以做一些初始化工作，例如建立一个和 HDFS 的连接
        subtask = getRuntimeContext.getIndexOfThisSubtask // 获取子任务编号
      }

      override def flatMap(in: SensorReading, out: Collector[(Int, String, Long, Double)]): Unit = {
        out.collect((subtask, in.id, in.timestamp, in.temperature))
      }

      override def close(): Unit = {
        // 这里可以做一些清理工作，例如断开和 HDFS 的连接
        subtask = 0
      }
    }).print("myRichFlatMap")

    env.execute("learn transform")
  }
}
