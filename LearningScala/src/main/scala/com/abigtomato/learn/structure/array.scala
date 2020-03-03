package structure

import scala.collection.mutable.ArrayBuffer

object Learn {
  def main(args: Array[String]) {
    // 定长数组
    val arr = new Array[Int](10)

    // 变长数组
    val buf = ArrayBuffer[Int]()

    // Array和ArrayBuffer的互转（转换动作不会修改原数组，而是生成新数组）
    println(arr.toBuffer)
    println(buf.toArray)

    // 数组元素赋值
    for(index <- 0 until arr.length) arr(index) = index * index

    // 数组遍历
    arr.foreach((elem: Int) => print(elem + " "))
    println()

    // ofDim生成二维数组
    val matrix1 = Array.ofDim[Double](3, 4)
    for(i <- 0 until matrix1.length; j <- 0 until matrix1(i).length) print(matrix1(i)(j) + " ")
    println()

    // 手动生成二维数组
    val matrix2 = new Array[Array[Int]](10)
    for(index <- 0 until matrix2.length) matrix2(index) = new Array[Int](3)
    for(i <- 0 until matrix2.length; j <- 0 until matrix2(i).length) matrix2(i)(j) = i * j
    for(i <- 0 until matrix2.length; j <- 0 until matrix2(i).length) print(matrix2(i)(j) + " ")
    println()
  }
}