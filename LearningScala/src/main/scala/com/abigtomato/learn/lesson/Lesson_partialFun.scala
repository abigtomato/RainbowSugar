package lesson

/**
  * Scala中偏函数相当于java中的switch...case...
  * 偏函数中需要匹配一种类型，匹配上之后返回一种类型。
  */
object Lesson_partialFun {

  def myPartial:PartialFunction[Int,String] = {
    case 15 =>{"value is 15"}
    case 100 =>{"value is 100"}
    case _=>{"no ... match"}
  }

  def main(args: Array[String]): Unit = {
    val str: String = myPartial(15)
    println(str)
  }
}
