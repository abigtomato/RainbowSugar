package lesson

/**
  *
  * set  无序有自动去重功能
  */
object Lesson_set {
  def main(args: Array[String]): Unit = {
    /**
      * 定义
      */
//    val set = Set[Int](1,2,3,4,5,5)

    /**
      * 遍历 for | foreach
      */
//    set.foreach(println)
//    for(elem <- set){
//      println(elem)
//    }

    /**
      * 方法
      */
//    val result = set.filter(i=>{i>3})
//    println(result)
//    val set1 = Set[Int](1,2,3,4,5)
//    val set2 = Set[Int](1,2,3,7,8)
////    val result = set1 &~ set2
//    val result = set2.diff(set1)
//    result.foreach(println)

//    val result: Set[Int] = set1.intersect(set2)
//    val result: Set[Int] = set1 & set2
//    result.foreach(println)

    /**
      * set 可变
      */
    val set = scala.collection.mutable.Set[Int]()
    set.+=(1)
    set.+=(2)
    set.+=(100)
    set.foreach(println)
    val set1 = scala.collection.immutable.Set[Int]()


  }
}
