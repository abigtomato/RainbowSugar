package lesson

/**
  * tuple注意：
  * 1.tuple 与列表一样，只不过tuple中的每个元素都有一个类型
  * 2.tuple创建时，可以new,可以不new
  * 3.tuple最多支持22个元素
  * 4.tuple取值使用 tuple._xx
  * 5.tuple遍历使用 tuple.productIterator
  * 6.map中的一个个元素就是二元组
  */
object Lesson_tuple {
  def main(args: Array[String]): Unit = {
    /**
      * tuple 定义
      */
    val tuple1 = new Tuple1("hello")
    val tuple2: (Int, String) = new Tuple2(1, "zhangsan")
//    val tuple3: (Int, Boolean, Char) = new Tuple3(1, true, 'c')
    val tuple3 = new Tuple3(1, true, 'c')
    val tuple4 = Tuple4(1,2,true,"hello")
    val tuple6 = (1,2,3,4,false,100)
    val tuple22 = (1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22)
    //    val tuple22: (Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Boolean, Int, Int, Int, Int, Int, Int) = Tuple22(1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, true, 17, 18, 19, 20, 21, 22)
    //    val tuple22 = new Tuple22(1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22)

    /**
      * tuple遍历
      */
//    val iter: Iterator[Any] = tuple6.productIterator
    //    while(iter.hasNext){
    //      val unit = iter.next()
    //      println(unit)
    //    }

    /**
      * tuple方法
      */
      println(tuple22.toString())
    val swap: (String, Int) = tuple2.swap
    println(swap)


  }
}
