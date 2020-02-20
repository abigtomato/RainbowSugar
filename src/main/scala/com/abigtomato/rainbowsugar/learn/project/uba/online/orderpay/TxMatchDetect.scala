package com.abigtomato.rainbowsugar.learn.project.uba.online.orderpay

import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.co.CoProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

/**
 * 需求：来自两条流的订单交易匹配：对于订单支付事件，用户支付完成后还需确认平台账户上是否到账
 */
object TxMatchDetect {

  // 用于输出只有支付事件，但无相应到账事件的数据
  val unmatchedPays = new OutputTag[OrderEvent]("unmatchedPays")
  // 用于输出只有到账事件，但无相应支付事件的数据
  val unmatchedReceipts = new OutputTag[ReceiptEvent]("unmatchedReceipts")

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    // 订单支付事件流
    val orderEventStream = env
      .readTextFile(getClass.getResource("/OrderLog.csv").getPath)
      .map(data => {
        val dataArray = data.split(",")
        OrderEvent(dataArray(0).trim.toLong, dataArray(1).trim, dataArray(2).trim, dataArray(3).trim.toLong)
      })
      .filter(_.txId != "")
      .assignAscendingTimestamps(_.eventTime * 1000L)
      .keyBy(_.txId)

    // 支付到账事件流
    val receiptEventStream = env
      .readTextFile(getClass.getResource("/ReceiptLog.csv").getPath)
      .map(data => {
        val dataArray = data.split(",")
        ReceiptEvent(dataArray(0).trim, dataArray(1).trim, dataArray(2).trim.toLong)
      })
      .assignAscendingTimestamps(_.eventTime * 1000L)
      .keyBy(_.txId)

    // 两个事件流通过相同的key分组后，进行connect连接
    val processedStream = orderEventStream
      .connect(receiptEventStream)
      // 对连接后的流进行自定义process处理
      .process(new TxPayMatch())

    processedStream.print("matched")
    processedStream.getSideOutput(unmatchedPays).print("unmatchedPays")
    processedStream.getSideOutput(unmatchedReceipts).print("unmatchedReceipts")

    env.execute("tx match job")
  }

  // 自定义连接流的处理
  class TxPayMatch() extends CoProcessFunction[OrderEvent, ReceiptEvent, (OrderEvent, ReceiptEvent)] {

    // 定义状态保存已经到达的订单支付事件
    lazy val payState: ValueState[OrderEvent] = getRuntimeContext.getState(new ValueStateDescriptor[OrderEvent]("pay-state", classOf[OrderEvent]))
    // 定义状态保存已经到达的支付到账事件
    lazy val receiptState: ValueState[ReceiptEvent] = getRuntimeContext.getState(new ValueStateDescriptor[ReceiptEvent]("receipt-state", classOf[ReceiptEvent]))

    // 订单支付事件数据的处理
    override def processElement1(pay: OrderEvent, ctx: CoProcessFunction[OrderEvent, ReceiptEvent, (OrderEvent, ReceiptEvent)]#Context, out: Collector[(OrderEvent, ReceiptEvent)]): Unit = {
      val receipt = receiptState.value()
      if (receipt != null) {
        // 1.若pay事件到达，并且receipt事件也存在，则输出正常匹配结果
        out.collect((pay, receipt))
        receiptState.clear()  // 匹配成功后清除本次数据，让状态保存下一次数据
      } else {
        // 2.若pay事件到达，receipt事件不存在，则保存pay的状态并注册定时器等待对应事件的到来
        payState.update(pay)
        ctx.timerService().registerEventTimeTimer(pay.eventTime * 1000L + 5000L)
      }
    }

    // 支付到账事件数据的处理
    override def processElement2(receipt: ReceiptEvent, ctx: CoProcessFunction[OrderEvent, ReceiptEvent, (OrderEvent, ReceiptEvent)]#Context, out: Collector[(OrderEvent, ReceiptEvent)]): Unit = {
      val pay = payState.value()
      if (pay != null) {
        // 1.若receipt事件到达，并且pay事件也存在，则输出正常匹配结果
        out.collect((pay, receipt))
        payState.clear()
      } else {
        // 2.若receipt事件到达，pay事件不存在，则保存receipt的状态并注册定时器等待对应事件的到来
        receiptState.update(receipt)
        ctx.timerService().registerEventTimeTimer(receipt.eventTime * 1000L + 5000L)
      }
    }

    override def onTimer(timestamp: Long, ctx: CoProcessFunction[OrderEvent, ReceiptEvent, (OrderEvent, ReceiptEvent)]#OnTimerContext, out: Collector[(OrderEvent, ReceiptEvent)]): Unit = {
      // 1.若定时器触发，pay状态不存在，则表示receipt事件没有匹配到对应的pay
      if (payState.value() != null) {
        ctx.output(unmatchedPays, payState.value())
      }
      // 2.若定时器触发，receipt状态不存在，则表示pay事件没有匹配到对应的receipt
      if (receiptState.value() != null) {
        ctx.output(unmatchedReceipts, receiptState.value())
      }
      // 3.清除本次数据的状态，让状态为下一次数据服务
      payState.clear()
      receiptState.clear()
    }
  }
}