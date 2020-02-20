package com.abigtomato.rainbowsugar.learn.project.uba.online.hotitems

import java.sql.Timestamp
import java.util.Properties

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.util.Collector

import scala.collection.mutable.ListBuffer

/**
 * 需求：每隔5分钟输出最近1小时内点击量最多的前N个商品
 */
object HotItems {

  def main(args: Array[String]): Unit = {
    // 1.执行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1) // 设置并行度
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime) // 设置事件时间窗口

    // 2.数据源操作
    val properties = new Properties()
    properties.setProperty("bootstrap.servers", "192.168.121.100:9092")
    properties.setProperty("group.id", "consumer-group")
    properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty("auto.offset.reset", "latest")
    val kafkaStream = env.addSource(new FlinkKafkaConsumer[String]("hotitems", new SimpleStringSchema(), properties))

    // 3.转换操作
    val dataStream = kafkaStream
      .map(data => {
        val dataArray = data.split(",")
        UserBehavior(dataArray(0).trim.toLong, dataArray(1).trim.toLong, dataArray(2).trim.toInt, dataArray(3).trim, dataArray(4).trim.toLong)
      })
      .assignAscendingTimestamps(_.timestamp * 1000L) // 指定升序时间戳

    // 4.聚合操作
    val processedStream = dataStream
      .filter(_.behavior == "pv")
      .keyBy(_.itemId)  // 商品id分组
      .timeWindow(Time.hours(1), Time.minutes(5)) // 设置滑动窗口
      .aggregate(new CountAgg(), new WindowResult())  // 自定义窗口聚合
      .keyBy(_.windowEnd) // 窗口关闭时间分组
      .process(new TopNHotItems(3)) // 自定义组内聚合

    // 5.sink操作
    processedStream.print()

    env.execute("hot items job")
  }
}

// 累计聚合，AggregateFunction泛型：输入类型，聚合结果类型，输出类型
class CountAgg() extends AggregateFunction[UserBehavior, Long, Long] {

  override def createAccumulator(): Long = 0L // 初始化累加器

  override def add(value: UserBehavior, accumulator: Long): Long = accumulator + 1  // 累计操作

  override def getResult(accumulator: Long): Long = accumulator // 返回结果

  override def merge(a: Long, b: Long): Long = a + b  // 多分组合并操作
}

// 平均数聚合
class AverageAgg() extends AggregateFunction[UserBehavior, (Long, Int), Double] {

  override def createAccumulator(): (Long, Int) = (0L, 0)

  override def add(value: UserBehavior, accumulator: (Long, Int)): (Long, Int) = (accumulator._1 + value.timestamp, accumulator._2 + 1)

  override def getResult(accumulator: (Long, Int)): Double = accumulator._1 / accumulator._2

  override def merge(a: (Long, Int), b: (Long, Int)): (Long, Int) = (a._1 + b._1, a._2 + b._2)
}

// 自定义窗口输出
class WindowResult() extends WindowFunction[Long, ItemViewCount, Long, TimeWindow] {

  override def apply(key: Long, window: TimeWindow, input: Iterable[Long], out: Collector[ItemViewCount]): Unit = {
    out.collect(ItemViewCount(key, window.getEnd, input.iterator.next()))
  }
}

// 自定义组内聚合
class TopNHotItems(topSize: Int) extends KeyedProcessFunction[Long, ItemViewCount, String]{

  // 组内所有商品的状态
  private var itemState: ListState[ItemViewCount] = _

  // 初始化状态
  override def open(parameters: Configuration): Unit = {
    itemState = getRuntimeContext.getListState(new ListStateDescriptor[ItemViewCount]("item-state", classOf[ItemViewCount]))
  }

  override def processElement(value: ItemViewCount, ctx: KeyedProcessFunction[Long, ItemViewCount, String]#Context, out: Collector[String]): Unit = {
    itemState.add(value)
    // 注册定时器，延迟1秒触发（组内最后一条数据的后一秒）
    ctx.timerService().registerEventTimeTimer(value.windowEnd + 1)
  }

  // 定时器回调函数
  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, ItemViewCount, String]#OnTimerContext, out: Collector[String]): Unit = {
    // 包装组内数据为ListBuffer
    val allItems: ListBuffer[ItemViewCount] = new ListBuffer()
    import scala.collection.JavaConversions._
    for (item <- itemState.get()) {
      allItems += item
    }

    // 对数量进行降序排序，并取topN
    val sortedItems = allItems
      .sortBy(_.count)(Ordering.Long.reverse)
      .take(topSize)

    // 清空状态
    itemState.clear()

    // 格式化输出
    val result: StringBuilder = new StringBuilder()
    result.append("时间：").append(new Timestamp(timestamp - 1)).append("\n")
    for (index <- sortedItems.indices) {
      val currentItem = sortedItems(index)
      result.append("No").append(index + 1).append(":")
        .append(" 商品ID = ").append(currentItem.itemId)
        .append(" 浏览量 = ").append(currentItem.count)
        .append("\n")
    }
    result.append("==================================")

    // 输出延迟
    Thread.sleep(1000)
    out.collect(result.toString())
  }
}