package lesson

/**
  * Trait注意：
  *   1.trait类似于Java中接口和抽象类的整合
  *   2.trait与object类似不可以传参。
  *   3.类继承多个trait时，第一个关键字使用extends,之后使用with
  *   4.trait中可以定义方法实现和方法的不实现，也可以定义变量和常量
  */
trait Read{
  val score = 100
  def read(name:String) = {
    println(s"$name is reading... ... ")
  }
}
trait Listen{
  val score = 200
  def listen(name:String) = {
    println(s"$name is listening... ... ")
  }
}
class Person1() extends Read with Listen {
  override val score = 1000
}

object Lession_trait1 {
  def main(args: Array[String]): Unit = {
    val p1 = new Person1()
    p1.read("zhangsan")
    p1.listen("lisi")
    println(p1.score)
  }
}
