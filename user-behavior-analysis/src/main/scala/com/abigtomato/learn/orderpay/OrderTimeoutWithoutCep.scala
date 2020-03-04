package com.abigtomato.learn.orderpay

import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

/**
 * 需求：订单超时检测：超过15分钟没有支付成功，则输出报警信息（状态编程实现）
 */
object OrderTimeoutWithoutCep {

  // 定义侧输出流，用于输出超时状态
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

    // 自定义process function进行超时检测
//    val timeoutWarningStreatm = orderEventStream.process(new OrderTimeoutWarning())
//    timeoutWarningStreatm.print()

    // 自定义process function进行超时检测 V2
    val orderResultStream = orderEventStream.process(new OrderPayMatch())
    orderResultStream.print()
    orderResultStream.getSideOutput(orderTimeoutOutputTag).print("timeout")

    env.execute("order timeout cep job")
  }

  // 处理按orderId分组后的各组数据
  class OrderPayMatch() extends KeyedProcessFunction[Long, OrderEvent, OrderResult] {

    // 定义一个判断pay状态是否到来的状态
    lazy val isPayedState: ValueState[Boolean] = getRuntimeContext.getState(new ValueStateDescriptor[Boolean]("ispayed-state", classOf[Boolean]))

    // 定义一个pay状态的超时时间状态
    lazy val timerState: ValueState[Long] = getRuntimeContext.getState(new ValueStateDescriptor[Long]("timer-state", classOf[Long]))

    override def processElement(value: OrderEvent, ctx: KeyedProcessFunction[Long, OrderEvent, OrderResult]#Context, out: Collector[OrderResult]): Unit = {
      val isPayed = isPayedState.value()
      val timerTs = timerState.value()

      // 1.当前事件状态为create
      if (value.eventType == "create") {
        if (isPayed) {
          // 1.1 若pay状态已经到来（乱序数据，pay先到，create后到），则代表匹配成功，输出主流并清空所有定时器和状态
          out.collect(OrderResult(value.orderId, "payed successfully!"))
          ctx.timerService().deleteEventTimeTimer(timerTs)
          isPayedState.clear()
          timerState.clear()
        } else {
          // 1.2 若pay状态没有到来，注册定时器等待pay的到来
          val ts = value.eventTime * 1000L + (15 * 60 * 1000L)
          ctx.timerService().registerEventTimeTimer(ts)
          timerState.update(ts)
        }
      // 2.当前事件状态为pay
      } else if (value.eventType == "pay") {
        if (timerTs > 0) {
          // 2.1 若create事件已经到来（已经注册过定时器）（正常顺序，create先到，pay后到），则判断当前pay是否超时，并且清空所有定时器和状态
          if (timerTs > value.eventTime * 1000L) {
            // 2.1.1 若当前pay事件发生的时间没有超过定时器规定时间，则输出匹配成功
            out.collect(OrderResult(value.orderId, "payed successfully!"))
          } else {
            // 2.1.2 若当前pay已经超时，则输出到侧输出流
            ctx.output(orderTimeoutOutputTag, OrderResult(value.orderId, "payed but already timeout"))
          }
          ctx.timerService().deleteEventTimeTimer(timerTs)
          isPayedState.clear()
          timerState.clear()
        } else {
          // 2.2 若create事件没有到来
          isPayedState.update(true)
          // pay先到，create还没有到达，则表示数据为乱序，注册定时器等待create的到来（等待水位线涨到当前时间）
          ctx.timerService().registerEventTimeTimer(value.eventTime * 1000L)
          timerState.update(value.eventTime * 1000L)
        }
      }
    }

    override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, OrderEvent, OrderResult]#OnTimerContext, out: Collector[OrderResult]): Unit = {
      // 根据状态的值，判断哪个数据没到
      if (isPayedState.value()) {
        // 状态为true，表示pay先到，但没等到create
        ctx.output(orderTimeoutOutputTag, OrderResult(ctx.getCurrentKey, "already payed but not found create log."))
      } else {
        // 状态为false，表示create先到，但没等到pay
        ctx.output(orderTimeoutOutputTag, OrderResult(ctx.getCurrentKey, "order timeout."))
      }
      isPayedState.clear()
      timerState.clear()
    }
  }
}

class OrderTimeoutWarning() extends KeyedProcessFunction[Long, OrderEvent, OrderResult]{

  lazy val isPayedState: ValueState[Boolean] = getRuntimeContext.getState(new ValueStateDescriptor[Boolean]("ispayed-state", classOf[Boolean]))

  override def processElement(value: OrderEvent, ctx: KeyedProcessFunction[Long, OrderEvent, OrderResult]#Context, out: Collector[OrderResult]): Unit = {
    val isPayed = isPayedState.value()

    if (value.eventType == "create" && !isPayed) {
      ctx.timerService().registerEventTimeTimer(value.eventTime * 1000L + (15 * 60 * 1000L))
    } else if (value.eventType == "pay") {
      isPayedState.update(true)
    }
  }

  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, OrderEvent, OrderResult]#OnTimerContext, out: Collector[OrderResult]): Unit = {
    val isPayed = isPayedState.value()
    if (isPayed) {
      out.collect(OrderResult(ctx.getCurrentKey, "order payed successfully!"))
    } else {
      out.collect(OrderResult(ctx.getCurrentKey, "order timeout!"))
    }
    isPayedState.clear()
  }
}