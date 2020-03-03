package com.albert.api.process

import com.albert.api.source.SensorReading
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

object LearnSideOutput {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val socketStream = env.socketTextStream("localhost", 7777)

    val dataStream = socketStream.map(data => {
      val dataArr = data.split(",")
      SensorReading(dataArr(0).trim, dataArr(1).trim.toLong, dataArr(2).trim.toDouble)
    }).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[SensorReading](Time.seconds(1)) {
      override def extractTimestamp(element: SensorReading): Long = element.timestamp * 1000
    })

    // 冰点报警，如果传感器温度小于32F，则输出报警信息到侧输出流
    val processedStream = dataStream
      .process(new FreezingAlert())

    dataStream.print("dataStream")
    processedStream
      .getSideOutput(new OutputTag[String]("freezing alert"))
      .print("alert data")

    env.execute("side output test")
  }
}

class FreezingAlert() extends ProcessFunction[SensorReading, SensorReading]{

  // 定义一个侧输出流的标签
  lazy val alertOutput: OutputTag[String] = new OutputTag[String]("freezing alert")

  override def processElement(value: SensorReading, ctx: ProcessFunction[SensorReading, SensorReading]#Context, out: Collector[SensorReading]): Unit = {
    if (value.temperature < 32.0) {
      // 指定报警信息输出到侧输出流
      ctx.output(alertOutput, "freezing alert for " + value.id)
    } else {
      // 主输出流
      out.collect(value)
    }
  }
}
