package com.abigtomato.learn.basic

/**
 * match注意：
 *  - 1.match 不仅可以匹配值还可以匹配类型
 *  - 2. case _=>{"no ... match ...."} 代表默认匹配，什么都匹配不上再匹配，放在匹配最后。
 *  - 3.匹配过程中会有数据类型转换
 *  - 4.从上往下依次匹配，匹配上之后，就自动终止
 *  - 5.xx match {xxx} 相当于是一整行，方法的{...}可以省略
 */
object MatchCase1 {
  def main(args: Array[String]): Unit = {
    val tuple = (1,"hello",1.1,true,'c',2.5f)

    val iter = tuple.productIterator
    iter.foreach(MatchTest)
  }

  def MatchTest(o:Any): Unit =
    o match {
      case i: Int => println(s"type is Int ,value is $i")
      case 1 => println(s"value is 1")
      case s: String => println(s"type is String, value is $s")
      case d: Double => println(s"type is Double ,value is $d")
      case 'c' => println("value is c")
      case _ => println("no ... match ....")
    }
}

/**
 * 1.构造器中的每个参数都成为val；
 * 2.在伴生对象中提供apply方法，可以不使用new关键字实例对象；
 * 3.提供unapply方法让模式匹配工作；
 * 4.生成toString，equals，hashCode和copy方法；
 * 5.伴生类也可以定义方法字段来拓展。
 */
abstract class Amount

case class Dollar(value: Double) extends Amount

case class Currency(value: Double, unit: String) extends Amount

case object Nothing extends Amount

sealed abstract class TrafficLightColor

case object Red extends TrafficLightColor

case object Yellow extends TrafficLightColor

case object Green extends TrafficLightColor

object MatchCase2 {
  def main(args: Array[String]) {
    for(amt <- Array(Dollar(1000.0), Currency(1000.0, "EUR"), Nothing)) {
      val result = amt match {
        case Dollar(v) => "$" + v // 使用模式匹配来匹配类对象，并绑定属性值到变量中
        case Currency(_, u) => "Oh noes, I got" + u
        case Nothing => ""
      }
      println(amt + ":" + result)
    }

    // copy创建一个与现有对象值相同的新对象，并通过带名参数来修改某些属性
    val amt = Currency(29.95, "EUR")
    val price = amt.copy(value = 19.95)
    println(price)
    println(amt.copy(unit = "CHF"))

    // 模拟枚举
    for (color <- Array(Red, Yellow, Green)) {
      println(
        color match {
          case Red => "stop"
          case Yellow => "hurry up"
          case Green => "go"
        })
    }

    // 偏函数（必须使用花括号，只能处理与至少一个case匹配的输入）
    val fun: PartialFunction[Char, Int] = { case '+' => 1; case '-' => -1 }
    println(fun('+'))
    println(fun.isDefinedAt('0'))
  }
}