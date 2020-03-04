package com.abigtomato.learn.structure

import scala.collection.mutable

object QueueExamples {
  def main(args: Array[String]): Unit = {
    // 队列
    val q1 = new mutable.Queue[Int]

    // 入队
    q1 += 1
    q1 ++= List(2, 3)
    q1.enqueue(4, 5, 6)
    println(q1)

    println(q1.dequeue) // 出队
    println(q1.head)    // 查看队头
    println(q1.tail)    // 出队元素后返回队列
  }
}