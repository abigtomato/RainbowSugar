package lesson

/**
  * case class 注意：
  * 1.样例类中的参数就是当前类的属性，默认有getter，setter方法（定义时指定成var）
  * 2.样例类实现了toString,equals，copy和hashCode等方法。
  * 3.样例类可以new 可以不new
  */
case class Human(var name:String,age:Int)

object Lesson_caseclass {
  def main(args: Array[String]): Unit = {
    val h1 =  Human("zhangsan",18)
    val h2 =  Human("zhangsan",18)
    println(h1==h2)

  }
}
