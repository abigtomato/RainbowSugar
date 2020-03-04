package com.abigtomato.learn.structure

import scala.collection.mutable

object StackExamples {
  def main(args: Array[String]): Unit = {
    // 堆栈
    val s = new mutable.Stack[Int]

    s.push(1, 2, 3) // 压栈
    println(s)

    println(s.pop()) // 弹栈
    println(s.top)  // 查看栈顶
  }
}