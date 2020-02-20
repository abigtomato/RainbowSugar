package com.abigtomato.rainbowsugar.learn.project.uba.online.market

import java.text.SimpleDateFormat
import java.util.Date

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

/**
 * 需求：app市场推广统计（黑名单过滤），如果对同一个广告点击超过一定限额（比如 100 次），应该把该用户加入黑名单并报警
 */
object AdStatisticsByGeo {

  // 定义侧输出流的tag标签
  private val blackListOutputTag = new OutputTag[BlackListWarning]("blackList")

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val resource = getClass.getResource("/AdClickLog.csv")
    val adEventStream = env
      .readTextFile(resource.getPath)
      .map(data => {
        val dataArray = data.split(",")
        AdClickEvent(dataArray(0).trim.toLong, dataArray(1).trim.toLong, dataArray(2).trim, dataArray(3).trim, dataArray(4).trim.toLong)
      })
      .assignAscendingTimestamps(_.timestamp * 1000L)

    val filterBlackListStream = adEventStream
      // 先按照（用户，广告）分组，用于过滤黑名单用户
      .keyBy(data => (data.userId, data.adId))
      // 自定义黑名单过滤函数，process接受各分组后的所有数据，processElement函数则制定每条数据的处理规则
      .process(new FilterBlackListUser(100))

    val adCountStream = filterBlackListStream
      // 再按照省份分组
      .keyBy(_.province)
      // 并开启时间窗口
      .timeWindow(Time.hours(1), Time.seconds(5))
      // 自定义窗口聚合函数
      .aggregate(new AdCountAgg(), new AdCountResult())

    // 打印主流的统计信息，每隔5秒统计前一小时的各个省份的广告点击量
    adCountStream.print("count")
    // 根据标签获取侧输出流，打印其中的黑名单信息
    filterBlackListStream.getSideOutput(blackListOutputTag).print("blacklist")

    env.execute("ad statistics job")
  }

  // 自定义黑名单过滤函数
  class FilterBlackListUser(maxCount: Int) extends KeyedProcessFunction[(Long, Long), AdClickEvent, AdClickEvent]{

    // 保存当前用户对当前广告的点击量（用于判断黑名单）
    lazy val countState: ValueState[Long] = getRuntimeContext.getState(new ValueStateDescriptor[Long]("count-state", classOf[Long]))

    // 标记当前（用户，广告）作为的key是否第一次发送到黑名单
    lazy val isSentBlackList: ValueState[Boolean] = getRuntimeContext.getState(new ValueStateDescriptor[Boolean]("issent-state", classOf[Boolean]))

    // 保存定时器触发的时间戳，用于每日00:00清空重置黑名单
    lazy val resetTimer: ValueState[Long] = getRuntimeContext.getState(new ValueStateDescriptor[Long]("resettime-state", classOf[Long]))

    override def processElement(value: AdClickEvent, ctx: KeyedProcessFunction[(Long, Long), AdClickEvent, AdClickEvent]#Context, out: Collector[AdClickEvent]): Unit = {
      val curCount = countState.value()

      // 若是第一次处理，注册一个每天00:00时触发的定时器
      if (curCount == 0) {
        // 获取隔天00:00时
        val ts = (ctx.timerService().currentProcessingTime() / (1000 * 60 * 60 * 24) + 1) * (1000 * 60 * 60 * 24)
        resetTimer.update(ts) // 更新定时器触发的时间戳
        ctx.timerService().registerProcessingTimeTimer(ts)  // 注册定时器
      }

      // 若某用户对某广告的点击超过黑名单阈值
      if (curCount >= maxCount) {
        // 并且该（用户，广告）并没有加入黑名单
        if (!isSentBlackList.value()) {
          // 则将报警信息写入侧输出流，并更新黑名单加入标识，保证当前（用户，广告）重复加入黑名单
          isSentBlackList.update(true)
          ctx.output(blackListOutputTag, BlackListWarning(value.userId, value.adId, "Click over " + maxCount + " times today."))
        }
        // 黑名单用户不传递
        return
      }

      // 每次处理数据就增加一次点击数量
      countState.update(curCount + 1)
      // 传递正常用户数据
      out.collect(value)
    }

    override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[(Long, Long), AdClickEvent, AdClickEvent]#OnTimerContext, out: Collector[AdClickEvent]): Unit = {
      // 每天00:00时触发定时器，清空各状态
      if (timestamp == resetTimer.value()) {
        isSentBlackList.clear()
        countState.clear()
      }
    }
  }
}

// 自定义窗口聚合函数，制定各个窗口中数据的处理规则
class AdCountAgg() extends AggregateFunction[AdClickEvent, Long, Long]{
  override def createAccumulator(): Long = 0L

  override def add(value: AdClickEvent, accumulator: Long): Long = accumulator + 1L

  override def getResult(accumulator: Long): Long = accumulator

  override def merge(a: Long, b: Long): Long = a + b
}

// 自定义窗口输出函数
class AdCountResult() extends WindowFunction[Long, CountByProvince, String, TimeWindow]{
  override def apply(key: String, window: TimeWindow, input: Iterable[Long], out: Collector[CountByProvince]): Unit = {
//    out.collect(CountByProvince(new Timestamp(window.getEnd).toString, key, input.iterator.next()))
    out.collect(CountByProvince(formatTs(window.getEnd), key, input.iterator.next()))
  }

  private def formatTs(ts: Long) = {
    val df = new SimpleDateFormat("yyyy/MM/dd-HH:mm:ss")
    df.format(new Date(ts))
  }
}