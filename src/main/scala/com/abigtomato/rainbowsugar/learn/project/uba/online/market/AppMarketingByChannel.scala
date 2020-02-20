package com.abigtomato.rainbowsugar.learn.project.uba.online.market

import java.sql.Timestamp
import java.util.UUID
import java.util.concurrent.TimeUnit

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.source.{RichSourceFunction, SourceFunction}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import scala.util.Random

/**
 * 需求：app市场推广统计（分渠道统计），每10秒统计之前1小时的数据（如：不同网站上广告链接的点击量、APP的下载量）
 */
object AppMarketingByChannel {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val dataStream = env
      .addSource(new SimulatedEventSource())  // 从自定义数据源中读取数据
      .assignAscendingTimestamps(_.timestamp) // 设置时间戳字段（升序排序）
      .filter(_.behavior != "UNINSTALL")  // 过滤掉用户的卸载操作
      .map(data => ((data.channel, data.behavior), 1L)) // 转换成以渠道和行为做为key的数据
      .keyBy(_._1)  // 分组
      .timeWindow(Time.hours(1), Time.seconds(10))  // 开启滑动窗口
      .process(new MarketingCountByChannel()) // 自定义窗口聚合函数
    dataStream.print()

    env.execute("app marketing by channel job")
  }
}

// 自定义测试数据源
class SimulatedEventSource() extends RichSourceFunction[MarketingUserBehavior] {

  // 执行标志
  var running = true
  // 用户行为列表
  val behaviorTypes = Seq("CLICK", "DOWNLOAD", "INSTALL", "UNINSTALL")
  // app下载渠道列表
  val channelSets = Seq("wechat", "weibo", "appstore", "huaweistore")
  // 随机数生成器
  val rand = new Random()

  // run函数控制数据源生成的规则
  override def run(ctx: SourceFunction.SourceContext[MarketingUserBehavior]): Unit = {
    // 定义最大数据量
    val maxElements = Long.MaxValue
    // 定义当前数据量
    var count = 0L

    while(running && count < maxElements) {
      val id = UUID.randomUUID().toString
      val behavior = behaviorTypes(rand.nextInt(behaviorTypes.size))
      val channel = channelSets(rand.nextInt(channelSets.size))
      val ts = System.currentTimeMillis()
      ctx.collect(MarketingUserBehavior(id, behavior, channel, ts))

      count += 1
      TimeUnit.MILLISECONDS.sleep(10L)
    }
  }

  // cancel函数控制数据源生成何时结束
  override def cancel(): Unit = running = false
}

// 自定义的窗口聚合函数（当窗口关闭时会触发，对这段窗口内的所有数据做处理）
class MarketingCountByChannel() extends ProcessWindowFunction[((String, String), Long), MarketingViewCount, (String, String), TimeWindow] {

  override def process(key: (String, String), context: Context, elements: Iterable[((String, String), Long)], out: Collector[MarketingViewCount]): Unit = {
    val startTs = new Timestamp(context.window.getStart).toString
    val endTs = new Timestamp(context.window.getEnd).toString
    val channel = key._1
    val behavior = key._2

    // 去重操作
    var countSet = Set[(String, String)]()
    val iterator = elements.iterator
    while (iterator.hasNext) {
      countSet += iterator.next()._1
    }
    val count = countSet.size // 统计数量

    out.collect(MarketingViewCount(startTs, endTs, channel, behavior, count))
  }
}