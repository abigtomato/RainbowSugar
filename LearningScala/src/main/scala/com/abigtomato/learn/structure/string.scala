package structure

object Learn {
  def main(args:Array[String]) {
    // 字符串
    val str_01 = "hello world"
    val str_02 = "HELLO WORLD"
    println("按元素取下标:" + str_01.indexOf('o'))
    println("忽略大小写判断字符串是否相同:" + str_01.equalsIgnoreCase(str_02))
    println("判断字符串是否相同:" + str_01.equals(str_02))
    println(str_01==str_02)

    // 可变字符串
    val strBuilder = new StringBuilder("233")
    strBuilder.+('F') // 追加字符
    strBuilder += 'F'
    strBuilder.++=("I Love")  // 追加字符串
    strBuilder ++= " I Love"
    strBuilder.append(" Very")
    println(strBuilder)
  }
}