package com.abigtomato.learn.utils

import java.util.Date

import net.sf.json.JSONObject
import org.joda.time.format.{DateTimeFormat, DateTimeFormatter}

/**
 * 日期时间工具类
 * 使用joda实现，java的Date工具类存在线程安全问题
 */
object DateUtils {

  val TIME_FORMAT: DateTimeFormatter = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss")
  val DATE_FORMAT: DateTimeFormatter = DateTimeFormat.forPattern("yyyy-MM-dd")
  val DATE_KEY_FORMAT: DateTimeFormatter = DateTimeFormat.forPattern("yyyyMMdd")
  val DATE_TIME_FORMAT: DateTimeFormatter = DateTimeFormat.forPattern("yyyyMMddHHmm")

  /**
   * 判断time1是否在time2之前
   * @param time1
   * @param time2
   * @return
   */
  def before(time1: String, time2: String): Boolean = {
    if (TIME_FORMAT.parseDateTime(time1).isBefore(TIME_FORMAT.parseDateTime(time2))) {
      return true
    }
    false
  }

  /**
   * 判断time1是否在time2之后
   * @param time1
   * @param time2
   * @return
   */
  def after(time1: String, time2: String): Boolean = {
    if (TIME_FORMAT.parseDateTime(time1).isAfter(TIME_FORMAT.parseDateTime(time2))) {
      return true
    }
    false
  }

  /**
   * 计算两个时间点的差值
   * @param time1
   * @param time2
   * @return
   */
  def minus(time1: String, time2: String): Int = {
    ((TIME_FORMAT.parseDateTime(time1).getMillis - TIME_FORMAT.parseDateTime(time2).getMillis) / 1000).toInt
  }

  def getToday(): String = ""

  /**
   *
   * @param action_time
   * @return
   */
  def parseTime(action_time: String): Date = {
    TIME_FORMAT.parseDateTime(action_time).toDate
  }

  /**
   *
   * @param time
   * @return
   */
  def formatTime(time: Date): String = {
    time.toString
  }

  /**
   *
   * @param datetime
   * @return
   */
  def getDateHour(datetime: String): String = {
    val date = datetime.split(" ")(0)
    val hourMinuteSecond = datetime.split(" ")(1)
    val hour = hourMinuteSecond.split(":")(0)
    date + "_" + hour
  }
}

/**
 * 字符串工具类
 */
object StringUtils {

  def fulfuill(str: String): Any = ""

  /**
   * 判断是否为空
   *
   * @param str
   * @return
   */
  def isEmpty(str: String): Boolean = {
    str == null || str.length == 0
  }

  /**
   * 判断是否不为空
   * @param str
   * @return
   */
  def isNotEmpty(str: String): Boolean = {
    !isEmpty(str)
  }

  /**
   * 去除尾部逗号
   * @param str
   * @return
   */
  def trimComma(str: String): String = {
    if (str.last.equals(',')) {
      return str.substring(0, str.length - 1)
    }
    str
  }

  def in(arr: Array[String], str: String): Boolean = {
    if (arr.contains(str)) true else false
  }

  def equal(str1: String, str2: String): Boolean = {
    if (str1 equals str2) true else false
  }
}

/**
 * 参数工具类
 */
object ParamUtils {

  /**
   * 从JSON对象中提取指定字段
   * @param jsonObject  JSON对象
   * @param field       字段
   * @return
   */
  def getParam(jsonObject: JSONObject, field: String): String = {
    jsonObject.getString(field)
  }
}

object NumberUtils {

  /**
   *
   * @param beforeNumber
   * @param afterNumber
   * @param number
   * @return
   */
  def between(beforeNumber: Int, afterNumber: Int, number: Int): Boolean = {
    if (number >= beforeNumber && number <= afterNumber) true else false
  }
}