package com.abigtomato.learn.networkflow

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.triggers.{Trigger, TriggerResult}
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector
import redis.clients.jedis.Jedis

/**
 * 布隆过滤器实现uv统计
 */
object UvWithBloom {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val dataStream = env
      .readTextFile("D:\\WorkSpace\\javaproject\\user-behavior-analysis\\network-flow-analysis\\src\\main\\resources\\UserBehavior.csv")
      .map(data => {
        val dataArray = data.split(",")
        UserBehavior(dataArray(0).trim.toLong, dataArray(1).trim.toLong, dataArray(2).trim.toInt, dataArray(3).trim, dataArray(4).trim.toLong)
      })
      .assignAscendingTimestamps(_.timestamp * 1000L)
      .filter(_.behavior == "pv")
      .map(data => ("dummyKey", data.userId))
      .keyBy(_._1)
      .timeWindow(Time.hours(1))
      .trigger(new MyTrigger()) // 自定义窗口的触发规则
      .process(new UvCountWithBloom())  // 自定义窗口的处理规则
    dataStream.print()

    env.execute("")
  }
}

// 自定义布隆过滤器
class Bloom(size: Long) extends Serializable {

  // 初始化布隆过滤器
  private val cap = if (size > 0) size else 1 << 27

  def hash(value: String, seed: Int): Long = {
    var result = 0L
    // 最简单的hash算法，字符串每一位字符的ascii码值乘以seed后做叠加（seed是随机数种子）
    for (i <- 0 until value.length) {
      result = result * seed + value.charAt(i)
    }
    result & (cap - 1)
  }
}

// 自定义触发器（自定义窗口关闭条件）
class MyTrigger() extends Trigger[(String, Long), TimeWindow] {

  override def onElement(element: (String, Long), timestamp: Long, window: TimeWindow, ctx: Trigger.TriggerContext): TriggerResult = {
    // 窗口中每来一条数据，就触发窗口关闭并清空状态
    TriggerResult.FIRE_AND_PURGE
  }

  override def onProcessingTime(time: Long, window: TimeWindow, ctx: Trigger.TriggerContext): TriggerResult = TriggerResult.CONTINUE

  override def onEventTime(time: Long, window: TimeWindow, ctx: Trigger.TriggerContext): TriggerResult = TriggerResult.CONTINUE

  override def clear(window: TimeWindow, ctx: Trigger.TriggerContext): Unit = {}
}

// 自定义窗口处理函数
class UvCountWithBloom() extends ProcessWindowFunction[(String, Long), UvCount, String, TimeWindow] {

  // 初始化redis连接器
  lazy val jedis = new Jedis("192.168.121.100", 6379)
  // 初始化布隆过滤器
  lazy val bloom = new Bloom(1 << 29)

  override def process(key: String, context: Context, elements: Iterable[(String, Long)], out: Collector[UvCount]): Unit = {
    // 以窗口关闭时间做为hashset的key和位图的key
    val storeKey = context.window.getEnd.toString
    // 统计数量
    var count = 0L

    // count做为外层key，窗口关闭时间做为内层key，构建一个hset表（用于存放各时间段的数据）
    if (jedis.hget("count", storeKey) != null) {
      count = jedis.hget("count", storeKey).toLong
    }

    // 根据自定义的窗口触发规则，窗口中实际只有一条数据，所以可直接使用last取出
    val userId = elements.last._2.toString
    // 通过布隆过滤器计算出userId的hash，也就是在位图中的偏移量
    val offset = bloom.hash(userId, 61)

    // 判断用户是否存在位图中，以此来过滤重复用户
    val isExist = jedis.getbit(storeKey, offset)
    if (!isExist) {
      // 若不存在则将位图相应的位置置为1
      jedis.setbit(storeKey, offset, true);
      // 更新统计count的hset表（表示当前时间段内有新的用户操作）
      jedis.hset("count", storeKey, (count + 1).toString)
    } else {
      // 若存在则代表重复用户
      out.collect(UvCount(storeKey.toLong, count))
    }
  }
}