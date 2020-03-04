package com.abigtomato.learn.structure

import scala.collection.mutable

object SetExamples {
  def main(args: Array[String]): Unit = {
    // 集合
    val set1 = Set(1, 2, 3, 4, 4)
    val set2 = Set(1, 2, 5)
    
    // 遍历
    set1.foreach(elem => print(elem + " "))
    println()

    // 交集
    set1.intersect(set2).foreach(elem => print(elem + " "))
    set1.&(set2).foreach(elem => print(elem + " "))
    println()

    // 并集
    set1.diff(set2).foreach(elem => print(elem + " "))
    set1.&~(set2).foreach(elem => print(elem + " "))
    println()

    // 按分隔符转字符串
    println(set1.mkString("~"))

    // 可变集合
    val set3 = mutable.Set[Int]()
    set3.+=(1)
    set3.+=(2)
    set3.+=(100)
    set3.foreach(println)
    val set4 = scala.collection.immutable.Set[Int]()
  }
}