package com.abigtomato.learn.oop

/**
 * Trait注意：
 *  - 1.trait类似于Java中接口和抽象类的整合
 *  - 2.trait与object类似不可以传参。
 *  - 3.类继承多个trait时，第一个关键字使用extends,之后使用with
 *  - 4.trait中可以定义方法实现和方法的不实现，也可以定义变量和常量
 */
trait ISEql {
  def isEQL(o:Any): Boolean
  def isNotEQL(o:Any): Boolean = !isEQL(o)
}

class Point(xx: Int, xy: Int) extends ISEql {
  val x: Int = xx
  val y: Int = xy

  override def isEQL(o: Any): Boolean = {
    o.isInstanceOf[Point]&&o.asInstanceOf[Point].x == this.x
  }
}

trait Read{
  val score = 100
  def read(name:String): Unit = {
    println(s"$name is reading... ... ")
  }
}

trait Listen{
  val score = 200
  def listen(name:String): Unit = {
    println(s"$name is listening... ... ")
  }
}

class Person1() extends Read with Listen {
  override val score = 1000
}

object Trait {
  def main(args: Array[String]): Unit = {
    val p1 = new Point(1,2)
    val p2 = new Point(1,3)
    println(p1.isEQL(p2))
    println(p1.isNotEQL(p2))

    val p3 = new Person1()
    p3.read("zhangsan")
    p3.listen("lisi")
    println(p3.score)
  }
}
