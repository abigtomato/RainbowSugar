package com.abigtomato.rainbowsugar.learn.project.uba.online.loginfail

import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

import scala.collection.mutable.ListBuffer

/**
 * 需求：登录失败检测，2秒内连续2次登录失败，则输出报警信息（状态编程实现）
 */
object LoginFail {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val resource = getClass.getResource("/LoginLog.csv")
    val loginEventStream = env
      .readTextFile(resource.getPath)
      .map(data => {
        val dataArray = data.split(",")
        LoginEvent(dataArray(0).trim.toLong, dataArray(1).trim, dataArray(2).trim, dataArray(3).trim.toLong)
      })
      // 数据中的时间戳存在乱序，需要定义watermark延迟开窗，延迟5秒（根据数据观察）
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[LoginEvent](Time.seconds(5)) {
        // 原数据中时间戳的级别是秒级，窗口时间的处理级别是毫秒级，所以在指定时间戳字段的同时要乘以1000
        override def extractTimestamp(element: LoginEvent): Long = element.eventTime * 1000L
      })

    val warningStream = loginEventStream
      .keyBy(_.userId)
      .process(new LoginWarning(2))
    warningStream.print()

    env.execute("login fail detect job")
  }
}

class LoginWarning(maxFailTimes: Int) extends KeyedProcessFunction[Long, LoginEvent, Warning] {

  // 定义状态，保存登录失败的事件
  lazy val loginFailState: ListState[LoginEvent] = getRuntimeContext.getListState(new ListStateDescriptor[LoginEvent]("login-fail", classOf[LoginEvent]))

  override def processElement(value: LoginEvent, ctx: KeyedProcessFunction[Long, LoginEvent, Warning]#Context, out: Collector[Warning]): Unit = {
//    val loginFailList = loginFailState.get()
//
//    if (value.eventType == "fail") {
//      if (!loginFailList.iterator().hasNext) {
//        ctx.timerService().registerEventTimeTimer(value.eventTime * 1000L + 2000L)
//      }
//      loginFailState.add(value)
//    } else {
//      loginFailState.clear()
//    }

    if (value.eventType == "fail") {
      val iter = loginFailState.get().iterator()
      // 若本次登录失败，则判断状态中是否已经存在登录失败事件
      if (iter.hasNext) {
        // 取出已经存在的登录失败事件和本次事件对比
        val firstFail = iter.next()
        // 若两次登录失败事件时间间隔小于2，则输出报警信息
        if (value.eventTime < firstFail.eventTime + 2) {
          out.collect(Warning(value.userId, firstFail.eventTime, value.eventTime, "login fail in 2 seconds"))
        }
        // 清空状态并将本次登录失败的事件保存到状态中
        loginFailState.clear()
        loginFailState.add(value)
      } else {
        // 若状态中不存在登录失败事件，则判定为第一次失败，存入状态中
        loginFailState.add(value)
      }
    } else {
      // 若本次登录成功，则清空状态信息
      loginFailState.clear()
    }
  }

//  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, LoginEvent, Warning]#OnTimerContext, out: Collector[Warning]): Unit = {
//    val allLoginFails: ListBuffer[LoginEvent] = new ListBuffer[LoginEvent]()
//    val iter = loginFailState.get().iterator()
//    while (iter.hasNext) {
//      allLoginFails += iter.next()
//    }
//
//    if (allLoginFails.length >= maxFailTimes) {
//      out.collect(Warning(allLoginFails.head.userId, allLoginFails.head.eventTime, allLoginFails.last.eventTime, "login fail"))
//    }
//  }
}
