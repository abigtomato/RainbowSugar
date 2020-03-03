package lesson

/**
  * 隐式转换第一种：
  * 隐式值和隐式参数
  * 注意:
  *   1.同作用域内不能有相同类型的隐式值
  *   2.implicit 关键字必须放在隐式参数定义的开头
  *   3.一个方法只有一个参数是隐式转换参数时，那么可以直接定义implicit关键字修饰的参数，调用时直接创建类型不传入参数即可
  *   4.一个方法如果有多个参数，要实现部分参数的隐式转换,必须使用柯里化这种方式,隐式关键字出现在后面，只能出现一次
  */
object Lesson_implicit1 {
  implicit val name = "zhangsan"
  implicit val i = 10
  def showInfo(gender:Char)(implicit s:String,age:Int) = {
    println(s"name is $s , age = $age,gender is $gender")
  }
  def main(args: Array[String]): Unit = {
    showInfo('m')("lisi",30)
  }

}
