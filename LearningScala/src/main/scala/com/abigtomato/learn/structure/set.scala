package structure

object Learn {
  def main(args: Array[String]) {
    // 集合
    var set1 = Set(1, 2, 3, 4, 4)
    var set2 = Set(1, 2, 5)
    
    // 遍历
    set1.foreach(elem => print(elem + " "))
    println()

    // 交集
    set1.intersect(set2)
    set1.&(set2).foreach(elem => print(elem + " "))
    println()

    // 并集
    set1.diff(set2)
    set1.&~(set2).foreach(elem => print(elem + " "))
    println()

    // 按分隔符转字符串
    println(set1.mkString("~"))
  }
}