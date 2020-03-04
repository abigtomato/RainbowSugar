package com.abigtomato.learn.market

import java.sql.Timestamp

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

/**
 * 需求：app市场推广统计（总量统计），每10秒统计之前1小时的数据（如：不同网站上广告链接的点击量、APP的下载量）
 */
object AppMarketing {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val dataStream = env
      .addSource(new SimulatedEventSource())
      .assignAscendingTimestamps(_.timestamp)
      .filter(_.behavior != "UNINSTALL")
      .map(_ => ("dummyKey", 1L)) // 哑key，为了方便之后的分组操作，没有实际意义
      .keyBy(_._1)  // 对哑key进行分组，为了生成KeyedStream
      .timeWindow(Time.hours(1), Time.seconds(10))
      .aggregate(new CountAgg(), new MarketingCountTotal()) // 增量聚合函数
    dataStream.print()

    env.execute("app marketing job")
  }
}

// 自定义针对窗口的增量聚合函数（窗口内数据来一条处理一条）
class CountAgg() extends AggregateFunction[(String, Long), Long, Long] {

  override def createAccumulator(): Long = 0L

  override def add(value: (String, Long), accumulator: Long): Long = accumulator + 1

  override def getResult(accumulator: Long): Long = accumulator

  override def merge(a: Long, b: Long): Long = a + b
}

// 自定义窗口处理结果的函数（Agg类函数的输出就是该函数的输入，该函数在agg处理完窗口内所有数据后才会执行）
class MarketingCountTotal() extends WindowFunction[Long, MarketingViewCount, String, TimeWindow]{

  override def apply(key: String, window: TimeWindow, input: Iterable[Long], out: Collector[MarketingViewCount]): Unit = {
    val startTs = new Timestamp(window.getStart).toString
    val endTs = new Timestamp(window.getEnd).toString
    val count = input.iterator.next() // 统计数量
    out.collect(MarketingViewCount(startTs, endTs, "app marketing", "total", count))
  }
}