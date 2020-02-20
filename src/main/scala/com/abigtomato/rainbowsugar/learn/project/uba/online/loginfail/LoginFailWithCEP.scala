package com.abigtomato.rainbowsugar.learn.project.uba.online.loginfail

import java.util

import org.apache.flink.cep.PatternSelectFunction
import org.apache.flink.cep.scala.CEP
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

/**
 * 需求：登录失败检测，2秒内连续2次登录失败，则输出报警信息（CEP实现）
 */
object LoginFailWithCEP {

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
      .keyBy(_.userId)

    // 定义匹配模式（针对无限流中的所有数据）
    val loginFailPattern = Pattern
      // 定义开始事件
      .begin[LoginEvent]("begin").where(_.eventType == "fail")
      // 定义接下来的事件，严格近邻（表示next事件必须紧挨着begin事件发生）
      .next("next").where(_.eventType == "fail")
      // 所有事件发生的时间限制，2秒之内
      .within(Time.seconds(2))

    // 在事件流上运用模式，得到一个模式流
    val patternStream = CEP.pattern(loginEventStream, loginFailPattern)

    // 在模式流上应用select function，检出匹配事件的序列
    val loginFailDataStream = patternStream.select(new LoginFailMatch())
    loginFailDataStream.print()

    env.execute("login fail with cep job")
  }
}

// 自定义模式流上的select，针对每一个匹配的模式做处理
class LoginFailMatch() extends PatternSelectFunction[LoginEvent, Warning] {

  // map的key存储事件的名称，value存储事件的原始数据
  override def select(map: util.Map[String, util.List[LoginEvent]]): Warning = {
    val firstFail = map.get("begin").iterator().next()
    val lastFail = map.get("next").iterator().next()
    Warning(firstFail.userId, firstFail.eventTime, lastFail.eventTime, "login fail!")
  }
}