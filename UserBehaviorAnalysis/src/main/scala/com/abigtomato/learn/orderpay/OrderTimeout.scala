package com.abigtomato.learn.orderpay

import java.util

import org.apache.flink.cep.scala.CEP
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.cep.{PatternSelectFunction, PatternTimeoutFunction}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

/**
 * 需求：订单超时检测：超过15分钟没有支付成功，则输出报警信息（CEP实现）
 */
object OrderTimeout {

  private val orderTimeoutOutputTag = new OutputTag[OrderResult]("orderTimeout")

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val resource = getClass.getResource("/OrderLog.csv")
    val orderEventStream = env
      .readTextFile(resource.getPath)
      .map(data => {
        val dataArray = data.split(",")
        OrderEvent(dataArray(0).trim.toLong, dataArray(1).trim, dataArray(2).trim, dataArray(3).trim.toLong)
      })
      .assignAscendingTimestamps(_.eventTime * 1000L)
      .keyBy(_.orderId)

    // 定义事件匹配模式
    val orderPayPattern = Pattern
      // 以create状态为开始事件
      .begin[OrderEvent]("begin").where(_.eventType == "create")
      // pay状态为begin的宽松依赖（两个事件中间可以间隔多个其他事件）
      .followedBy("follow").where(_.eventType == "pay")
      // 定义事件约束（两种事件的间隔时间）
      .within(Time.minutes(15))

    // 将匹配模式应用到原始数据流中生成模式流
    val patternStream = CEP.pattern(orderEventStream, orderPayPattern)

    // 针对模式流中的数据定义处理函数，参数：侧输出流tag，超时事件处理函数，事件处理函数
    val resultStream = patternStream.select(orderTimeoutOutputTag,
      new OrderTimeoutSelect(), new OrderPaySelect())

    resultStream.print("payed")
    resultStream.getSideOutput(orderTimeoutOutputTag).print("timeout")

    env.execute("order timeout job")
  }
}

// 自定义的超时事件处理
class OrderTimeoutSelect() extends PatternTimeoutFunction[OrderEvent, OrderResult] {

  override def timeout(map: util.Map[String, util.List[OrderEvent]], l: Long): OrderResult = {
    // 若是超时事件，则只有begin事件存在，follow不存在
    val timeoutOrderId = map.get("begin").iterator().next().orderId
    OrderResult(timeoutOrderId, "timeout!")
  }
}

// 自定义事件处理
class OrderPaySelect() extends PatternSelectFunction[OrderEvent, OrderResult] {

  override def select(map: util.Map[String, util.List[OrderEvent]]): OrderResult = {
    // 若是正常事件，则follow存在
    val payedOrderId = map.get("follow").iterator().next().orderId
    OrderResult(payedOrderId, "payed successfully!")
  }
}