package com.abigtomato.learn.analysis.session

import org.apache.spark.util.AccumulatorV2

import scala.collection.mutable

/**
 * 自定义累加器
 */
class SessionAccumulator extends AccumulatorV2[String, mutable.HashMap[String, Int]] {

  val countMap = new mutable.HashMap[String, Int]()

  // 判断累加器是否为空
  override def isZero: Boolean = this.countMap.isEmpty

  // 用于copy出和当前累加器相同的新累加器
  override def copy(): AccumulatorV2[String, mutable.HashMap[String, Int]] = {
    val acc = new SessionAccumulator
    acc.countMap ++= this.countMap  // ++= 拼接操作
    acc
  }

  // 清空累加器
  override def reset(): Unit = this.countMap.clear()

  // 累加操作
  override def add(v: String): Unit = {
    // key为空则添加k-v对
    if (this.countMap.contains(v)) this.countMap += (v -> 0)
    // key存在则更新key
    this.countMap.update(v, this.countMap(v) + 1)
  }

  // 控制累加器合并的操作
  override def merge(other: AccumulatorV2[String, mutable.HashMap[String, Int]]): Unit = {
    other match {
      case acc: SessionAccumulator => acc.countMap.foldLeft(this.countMap) {
        case (map, (k, v)) => map += (k -> (map.getOrElse(k, 0) + v))
      }
    }
  }

  // 累加器返回值
  override def value: mutable.HashMap[String, Int] = this.countMap
}
