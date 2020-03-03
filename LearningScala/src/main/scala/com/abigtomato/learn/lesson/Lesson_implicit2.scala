package lesson

/**
  * 1.隐式值 & 隐式参数
  * 2.隐式转换函数
  *   注意：隐式转换函数只与函数的参数类型和返回类型有关，
  *     与函数名称无关，所以作用域内不能有相同的参数类型和返回类型的不同名称隐式转换函数。
  */

class Bird(xname:String) {
  val name = xname
  def canFly() = {
    println(s"$name can fly ... ...")
  }
}

class Pig(xname:String) {
  val name = xname
}

object Lesson_implicit2 {

  implicit def pigToBird(p:Pig):Bird = {
    new Bird(p.name)
  }

  def main(args: Array[String]): Unit = {
    val pig = new Pig("xiaopig")
    pig.canFly()
  }
}
