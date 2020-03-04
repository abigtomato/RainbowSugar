package com.abigtomato.learn.structure

object StreamExamples {
  def main(args: Array[String]): Unit = {
    // 1. stream只有在需要的时候才会去计算下一个元素
    // 2. 是一个尾部被懒计算的不可变列表
    // 3. #:: 操作符用于返回流
    def numsForm(n: BigInt): Stream[BigInt] = n #:: numsForm(n + 1)
    val tenOrMore = numsForm(10)
    println(tenOrMore)

    println(tenOrMore.tail)
    println(tenOrMore.head)
    println(tenOrMore.tail.tail.tail)

    val squares = numsForm(5).map(x => x * x)
    println(squares)
    println(squares.take(5).force)

    println(tenOrMore)
  }
}