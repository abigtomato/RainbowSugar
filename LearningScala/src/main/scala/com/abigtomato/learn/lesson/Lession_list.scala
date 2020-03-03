package lesson

import scala.collection.mutable.ListBuffer

object Lession_list {
  def main(args: Array[String]): Unit = {
    /**
      * 定义
      */
//    val list = List[String]("a","b","c","c")

    /**
      * 遍历
      */
//    list.foreach(println)
//    for(elem <- list){
//      println(elem)
//    }
    /**
      * 方法
      *  map ： 一对一
      *  flatMap ： 一对多
      */
//    val list = List[String]("hello zhangsan","hello lisi","hello wangwu")
//
//    val i: Int = list.count(s=>{s.length>3})
//    println(i)
//    val strings: List[String] = list.filter(s => {
//      "hello zhangsan".equals(s)
//    })
//    strings.foreach(println)

//    val result1: List[Array[String]] = list.map(s=>{s.split(" ")})
//
//    val result2: List[String] = list.flatMap(s=>{s.split(" ")})
//    result2.foreach(println)


//    stringses.foreach(arr=>{
//      println("*********")
//      arr.foreach(println)
//    })


    /**
      * 可变list
      */

    val list = new ListBuffer[String]()
    list.append("a")
    list.append("b","c")
    list.foreach(println)


  }
}
