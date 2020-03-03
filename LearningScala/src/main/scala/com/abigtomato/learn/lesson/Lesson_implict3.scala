package lesson

/**
  * 隐式类
  *   注意：
  *     1.隐式类必须定义在包对象或者包类中
  *     2.隐式类的构造必须只有一个参数，同一个类，包对象，伴生对象中不能出现同类型构造的隐式类。
  */
class Pig1(xname:String){
  val name = xname
}

object Lesson_implict3 {
  implicit class Bird1(p:Pig1) {
    def canFly() ={
      println(s"${p.name} can fly... ... ")
    }
  }

  def main(args: Array[String]): Unit = {
    val p = new Pig1("xiaopig")
    p.canFly()
  }
}
