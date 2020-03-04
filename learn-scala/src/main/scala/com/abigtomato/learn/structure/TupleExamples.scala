package com.abigtomato.learn.structure

/**
 * tuple注意：
 *  - 1.tuple与列表一样，只不过tuple中的每个元素都有一个类型
 *  - 2.tuple创建时，可以new，以不new
 *  - 3.tuple最多支持22个元素
 *  - 4.tuple取值使用 tuple._xx
 *  - 5.tuple遍历使用 tuple.productIterator
 *  - 6.map中的一个个元素就是二元组
 */
object TupleExamples {
  def main(args: Array[String]): Unit = {
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