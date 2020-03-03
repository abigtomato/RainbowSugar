package lesson

object Lesson_string {
  def main(args: Array[String]): Unit = {
    val s1 = "abcde"
    val s2 = "ABCDE"

    println(s1.equals(s2))
    println(s1.equalsIgnoreCase(s2))
    println(s1.indexOf(96))

    val builder = new StringBuilder()
    builder.append("a")
    builder.append("b")
    builder.append(123)
    println(builder)
  }

}
