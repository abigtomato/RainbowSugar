package lesson

import scala.collection.mutable.ArrayBuffer

object Lession_array {
  def main(args: Array[String]): Unit = {
    /**
      * 定义array
      */
//    val arr1 = Array[String]("a","b","c")
//    val arr2 = new Array[String](3)
//    arr2(0) = "hello"
//    arr2(1) = "zhangsan"
//    arr2(2) = "lisi"

    /***
      * 遍历
      *  for  | foreach
      */
//    arr2.foreach(println)
//    arr1.foreach(println)
//    for(elem <- arr2){
//      println(elem)
//    }

    /**
      * 方法
      */
//    val arr1 = Array[Int](1,2,3)
//    val arr2 = Array[Int](4,5,6)
//    val ints: Array[Int] = Array.concat(arr1,arr2)
//    val result = ints.filter(i => {
//      i >= 5
//    })
//
//    result.foreach(println)

//    ints.foreach(println)

    /**
      * 可变长Array
      */
    val arr = new ArrayBuffer[String]()
    arr.append("a","b")
    arr.append("c")
    arr.foreach(println)


  }
}
