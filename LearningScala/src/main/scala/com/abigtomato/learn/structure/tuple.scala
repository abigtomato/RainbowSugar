package structure

object Learn {
  def main(args: Array[String]) {
    val tuple1 = Tuple2("word", 1)  // 二元组
    val tuple2 = Tuple3("Hello", 3.14, true)  // 三元组

    // 迭代
    val tupleIterator = tuple2.productIterator  // 取出迭代器
    while(tupleIterator.hasNext) print(tupleIterator.next() + " ")
    println()

    // 取值
    println(tuple2._1 + ":" + tuple2._2 + ":" + tuple2._3 + " ")

    // 翻转
    println(tuple1.swap)
  }
}