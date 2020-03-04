package com.abigtomato.learn.structure

import scala.collection.mutable

object MapExamples {
  def main(args: Array[String]): Unit = {
    // 不可变映射
    val map1 = Map("1" -> "Golang", 2 -> "Python", (3, "Scala"))
    // 可变映射
    val map2 = scala.collection.mutable.Map("Alice" -> 4, "Bob" -> 3, "Cindy" -> 8)
    // 空映射
    val map3 = new mutable.HashMap[String, Int]

    // 迭代
    val keyIterator = map1.keys.iterator  // 迭代器
    while(keyIterator.hasNext) {
      val key = keyIterator.next()
      print(key + " => " + map1.getOrElse(key, "no result") + " ")
    }
    println()

    for((k, v) <- map1) print(k + "=>" + v + " ")
    println()

    for(k <- map1.keys) print(k)
    println()
    
    for(v <- map1.values) print(v)
    println()

    map1.foreach(x => print(x._1 + ":" + x._2 + " "))
    println()

    // CRUD
    map2 += ("Docker" -> 4, "Kubernetes" -> 5)  // 新增
    map2 -= "Rust" // 移除
    map2("Rust") = 1  // 修改
    println(map2("Alice")) // 取值
    println(map2.contains("Bob")) // 包含

    // 对不可变映射做更新操作会返回新集合
    val newMap = map1 + ("4" -> "Rust")
    println(newMap)
  }
}