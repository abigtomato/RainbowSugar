package lesson

trait ISEql{
  def isEQL(o:Any) :Boolean
  def isNotEQL(o:Any) :Boolean = !isEQL(o)
}

class Point(xx:Int,xy:Int) extends ISEql {
  val x = xx
  val y = xy

  override def isEQL(o: Any): Boolean = {
    o.isInstanceOf[Point]&&o.asInstanceOf[Point].x==this.x
  }
}

object Lesson_trait {
  def main(args: Array[String]): Unit = {
    val p1 = new Point(1,2)
    val p2 = new Point(1,3)
    println(p1.isEQL(p2))
    println(p1.isNotEQL(p2))
  }
}
