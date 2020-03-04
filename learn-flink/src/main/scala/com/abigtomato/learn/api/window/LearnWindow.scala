package com.albert.api.window

import com.albert.api.source.SensorReading
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.functions.{AssignerWithPeriodicWatermarks, AssignerWithPunctuatedWatermarks}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.api.windowing.time.Time

object LearnWindow {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime) // 设置窗口的时间语义为事件时间
//    env.getConfig.setAutoWatermarkInterval(100L)  // 设置watermark生成的时间周期

    val socketStream = env.socketTextStream("localhost", 7777)

    val dataStream = socketStream.map(data => {
      val dataArr = data.split(",")
      SensorReading(dataArr(0).trim, dataArr(1).trim.toLong, dataArr(2).trim.toDouble)
    })
//      .assignAscendingTimestamps(_.timestamp * 1000)  // 用于有序且升序数据的时间窗口
      // 指定时间戳和watermark，传递的参数是watermark的延迟时间
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[SensorReading](Time.seconds(1)) {
        // 从data中提取时间戳，watermark默认200毫秒生成一次
        override def extractTimestamp(element: SensorReading): Long = element.timestamp * 1000
      })

    // 统计15内的最小温度，每隔5秒输出一次结果
    val minTempPerWindowStream = dataStream
      .map(data => (data.id, data.temperature))
      .keyBy(_._1)
      .timeWindow(Time.seconds(10), Time.seconds(5))  // 设置滑动时间窗口，窗口大小为10，步长为5
      .reduce((data1, data2) => (data1._1, data1._2.min(data2._2)))
    minTempPerWindowStream.print("min temp")
    dataStream.print("input data")

    env.execute("window test")
  }
}

class MyAssigner() extends AssignerWithPeriodicWatermarks[SensorReading] {

  val bound: Int = 60 * 1000 // 延迟时间
  var maxTs: Long = Long.MinValue // 最大时间戳

  // 周期性生成的watermark（最大时间戳 - 延迟时间）
  override def getCurrentWatermark: Watermark = new Watermark(maxTs - bound)

  override def extractTimestamp(element: SensorReading, previousElementTimestamp: Long): Long = {
    // 周期性重新计算时间窗口内的最大时间戳
    maxTs = maxTs.max(element.timestamp * 1000)
    // 提取data中的时间戳字段
    element.timestamp * 1000
  }
}

class MyAssigner2() extends AssignerWithPunctuatedWatermarks[SensorReading] {

  // 每有一个数据就生成一个watermark
  override def checkAndGetNextWatermark(lastElement: SensorReading, extractedTimestamp: Long): Watermark = new Watermark(extractedTimestamp)

  // 提取data中的时间戳字段
  override def extractTimestamp(element: SensorReading, previousElementTimestamp: Long): Long = element.timestamp
}
