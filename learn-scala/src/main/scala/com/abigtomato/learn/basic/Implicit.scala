package com.abigtomato.learn.basic

/**
 * 隐式转换第一种：
 * 隐式值和隐式参数
 * 注意:
 *   1.同作用域内不能有相同类型的隐式值
 *   2.implicit 关键字必须放在隐式参数定义的开头
 *   3.一个方法只有一个参数是隐式转换参数时，那么可以直接定义implicit关键字修饰的参数，调用时直接创建类型不传入参数即可
 *   4.一个方法如果有多个参数，要实现部分参数的隐式转换,必须使用柯里化这种方式,隐式关键字出现在后面，只能出现一次
 */
object Implicit1 {

  implicit val name: String = "zhangsan"
  implicit val i: Int = 10

  def showInfo(gender:Char)(implicit s: String, age: Int): Unit = {
    println(s"name is $s, age = $age,gender is $gender")
  }

  def main(args: Array[String]): Unit = {
    showInfo('m')("lisi",30)
  }
}

/**
 * 1.隐式值 & 隐式参数
 * 2.隐式转换函数
 *   注意：隐式转换函数只与函数的参数类型和返回类型有关，
 *     与函数名称无关，所以作用域内不能有相同的参数类型和返回类型的不同名称隐式转换函数。
 */
class Bird(xname: String) {

  val name: String = xname

  def canFly(): Unit = {
    println(s"$name can fly ... ...")
  }
}

class Pig(xname: String) {
  val name: String = xname
}

object Implicit2 {

  implicit def pigToBird(p:Pig):Bird = {
    new Bird(p.name)
  }

  def main(args: Array[String]): Unit = {
    val pig = new Pig("xiaopig")
    pig.canFly()
  }
}

/**
 * 隐式类
 *   注意：
 *     1.隐式类必须定义在包对象或者包类中
 *     2.隐式类的构造必须只有一个参数，同一个类，包对象，伴生对象中不能出现同类型构造的隐式类。
 */
class Pig1(xname: String) {

  val name: String = xname
}

object Implicit3 {

  implicit class Bird1(p:Pig1) {
    def canFly(): Unit ={
      println(s"${p.name} can fly... ... ")
    }
  }

  def main(args: Array[String]): Unit = {
    val p = new Pig1("xiaopig")
    p.canFly()
  }
}