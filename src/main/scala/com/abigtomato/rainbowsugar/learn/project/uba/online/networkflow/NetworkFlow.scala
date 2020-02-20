package com.abigtomato.rainbowsugar.learn.project.uba.online.networkflow

import java.sql.Timestamp
import java.text.SimpleDateFormat

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import scala.collection.mutable.ListBuffer

/**
 * 需求：每隔5秒，输出最近10分钟内访问量最多的前N个URL
 */
object NetworkFlow {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val dataStream = env
      .readTextFile("D:\\WorkSpace\\javaproject\\user-behavior-analysis\\network-flow-analysis\\src\\main\\resources\\apache.log")
      .map(data => {
        val dataArray = data.split(" ")
        val simpleDateFormat = new SimpleDateFormat("dd/MM/yyyy:HH:mm:ss")
        val timestamp = simpleDateFormat.parse(dataArray(3).trim).getTime
        ApacheLogEvent(dataArray(0).trim, dataArray(1).trim, timestamp, dataArray(5).trim, dataArray(6).trim)
      })
      .filter(data => {
        val pattern = "^*.(css|js|ico|png)$".r
        (pattern findFirstIn data.url).nonEmpty
      })
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[ApacheLogEvent](Time.seconds(1)) {
        override def extractTimestamp(element: ApacheLogEvent): Long = element.eventTime
      })
      .keyBy(_.url)
      .timeWindow(Time.minutes(10), Time.seconds(5))
      .allowedLateness(Time.seconds(60))
      .aggregate(new CountAgg(), new WindowResult())
      .keyBy(_.windowEnd)
      .process(new TopNHotUrls(5))
    dataStream.print()

    env.execute("network flow job")
  }
}

class CountAgg() extends AggregateFunction[ApacheLogEvent, Long, Long] {

  override def createAccumulator(): Long = 0L

  override def add(value: ApacheLogEvent, accumulator: Long): Long = accumulator + 1

  override def getResult(accumulator: Long): Long = accumulator

  override def merge(a: Long, b: Long): Long = a + b
}

class WindowResult() extends WindowFunction[Long, UrlViewCount, String, TimeWindow] {

  override def apply(key: String, window: TimeWindow, input: Iterable[Long], out: Collector[UrlViewCount]): Unit = {
    out.collect(UrlViewCount(key, window.getEnd, input.iterator.next()))
  }
}

class TopNHotUrls(topSize: Int) extends KeyedProcessFunction[Long, UrlViewCount, String] {

  lazy val urlState: ListState[UrlViewCount] = getRuntimeContext.getListState(new ListStateDescriptor[UrlViewCount]("url-state", classOf[UrlViewCount]))

  override def processElement(value: UrlViewCount, ctx: KeyedProcessFunction[Long, UrlViewCount, String]#Context, out: Collector[String]): Unit = {
    urlState.add(value)
    ctx.timerService().registerEventTimeTimer(value.windowEnd + 1)
  }

  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, UrlViewCount, String]#OnTimerContext, out: Collector[String]): Unit = {
    val allUrlViews = new ListBuffer[UrlViewCount]()
    val iter = urlState.get().iterator()
    while (iter.hasNext) {
      allUrlViews += iter.next()
    }

    urlState.clear()

    val sortedUrlViews = allUrlViews
      .sortWith(_.count > _.count)
      .take(topSize)

    val result = new StringBuilder
    result.append("[====================================\n")
    result.append("时间：").append(new Timestamp(timestamp - 1)).append("\n")
    for (i <- sortedUrlViews.indices) {
      val currentUrlView: UrlViewCount = sortedUrlViews(i)
      result.append("No").append(i+1).append(":")
        .append(" URL = ").append(currentUrlView.url)
        .append(" 流量 = ").append(currentUrlView.count).append("\n")
    }
    result.append("====================================]\n\n")

    // 控制输出频率，模拟实时滚动结果
    Thread.sleep(1000)
    out.collect(result.toString)
  }
}