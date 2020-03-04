package com.abigtomato.learn.structure

import scala.collection.mutable

object ListExamples {
  def main(args: Array[String]): Unit = {
    // 列表
    val list = List(1, 2, 3, 4, 5, 6)

    println(list.sliding(3))  // 切片
    println(list.head)        // 查看头部元素
    println(list.tail)        // 删除头部并返回操作后的列表

    println(9 :: List(4, 2))         // 9添加到列表头部
    println(9 :: 4 :: 2 :: Nil)      // 连接操作
    println(9 :: (4 :: (2 :: Nil)))  // 按顺序的连接操作

    // 过滤
    val filter = list.filter(elem => elem > 3)
    filter.foreach(elem => print(elem + " "))
    println()

    // 计数
    val count = list.count(elem => elem > 1)
    println(count)

    // 映射
    val list2 = List("hello golang", "hello scala", "hello python")

    val mapper = list2.map(elem => elem.split(" "))
    mapper.foreach(elem => elem.foreach(elem => print(elem + " ")))
    println()

    println(list2.map(_.toUpperCase))

    // 映射+压平
    list2.flatMap(elem => elem.split(" ")).foreach(elem => print(elem + " "))

    def ulcase(str: String) = Vector(str.toUpperCase(), str.toLowerCase())
    list2.flatMap(ulcase)
    println()

    // 化简，折叠
    println(List(1, 7, 2, 9).reduceLeft(_ - _))   // 从左边开始进行元素聚合
    println(List(1, 7, 2, 9).reduceRight(_ - _))  // 从右边开始进行元素聚合
    println(List(1, 7, 2, 9).foldLeft(10)(_ - _))  // 指定初值从左边开始进行元素聚合

    // 扫描（得到中间结果的集合）
    println((1 to 10).scanLeft(0)(_ + _))

    // 统计词频
    val freq = mutable.Map[Char, Int]()
    for(ch <- "Mississippi") freq(ch) = freq.getOrElse(ch, 0) + 1
    println(freq)

    // 拉链
    println(List(5.01, 2.67, 9.25) zip List(10, 2, 7))  // 两个列表元素一对一组合成元组
    println("Scala".zipWithIndex) // 字符和对应下标组合成元组

    // 懒视图（不会执行计算，需要take操作）
    val palindromicSquares = (1 to 10).view.map(x => {
      println(x)
      x * x
    }).filter(x => {
      x.toString == x.toString.reverse
    })
    println(palindromicSquares.take(3).mkString(",")) // take取3个元素做计算

    // 并行集合（底层使用分支合并线程池）
    (1 to 5).par.foreach{ elem => println(Thread.currentThread); println("^" + elem) }
    println((1 to 5).par.map(_ + 100).seq)
  }
}