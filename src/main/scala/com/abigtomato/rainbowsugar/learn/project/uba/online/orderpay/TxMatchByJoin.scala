package com.abigtomato.rainbowsugar.learn.project.uba.online.orderpay

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

/**
 * 需求：来自两条流的订单交易匹配：对于订单支付事件，用户支付完成后还需确认平台账户上是否到账（Join实现）
 */
object TxMatchByJoin {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val orderEventStream = env
      .readTextFile(getClass.getResource("/OrderLog.csv").getPath)
      .map(data => {
        val dataArray = data.split(",")
        OrderEvent(dataArray(0).trim.toLong, dataArray(1).trim, dataArray(2).trim, dataArray(3).trim.toLong)
      })
      .filter(_.txId != "")
      .assignAscendingTimestamps(_.eventTime * 1000L)
      .keyBy(_.txId)

    val receiptEventStream = env
      .readTextFile(getClass.getResource("/ReceiptLog.csv").getPath)
      .map(data => {
        val dataArray = data.split(",")
        ReceiptEvent(dataArray(0).trim, dataArray(1).trim, dataArray(2).trim.toLong)
      })
      .assignAscendingTimestamps(_.eventTime * 1000L)
      .keyBy(_.txId)

    // 订单支付事件流 interval join 支付到账事件流（两条流都按相同的key分组）
    val processedStream = orderEventStream
      .intervalJoin(receiptEventStream)
      // 设置订单支付事件对应的支付到账事件的区间（一条支付事件对应一条到账事件的前5秒到后5秒的数据）
      .between(Time.seconds(-5), Time.seconds(5))
      .process(new TxPayMatchByJoin())
    processedStream.print()

    env.execute("tx pay match by join job")
  }
}

// 自定义join处理
class TxPayMatchByJoin() extends ProcessJoinFunction[OrderEvent, ReceiptEvent, (OrderEvent, ReceiptEvent)] {

  override def processElement(left: OrderEvent, right: ReceiptEvent, ctx: ProcessJoinFunction[OrderEvent, ReceiptEvent, (OrderEvent, ReceiptEvent)]#Context, out: Collector[(OrderEvent, ReceiptEvent)]): Unit = {
    out.collect((left, right))
  }
}