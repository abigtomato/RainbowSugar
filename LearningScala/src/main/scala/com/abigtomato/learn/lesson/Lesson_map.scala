package lesson

object Lesson_map {
  def main(args: Array[String]): Unit = {
    /**
      * map定义
      */
//    val map = Map[String,Int]("a"->10,"b"->20,"c"->30,("d",40),("d",100))

    /**
      * 遍历
      */
    //    for(elem <- map){
    //      println(elem)
    //    }
    //    map.foreach(println)
    /**
      * map 获取值
      */
    //    val result: Option[Int] = map.get("a")
    //    val result: Int = map.get("a").getOrElse(1000)
    //    println(result)
    //获取keys
    //    val keys: Iterable[String] = map.keys
    //    for(key <- keys){
    //      println(s"key = $key ,value = ${map.get(key).get}")
    //    }
    //获取所有values
    //    val values: Iterable[Int] = map.values
    //    for(value <- values){
    //      println(s"$value")
    //    }

    /**
      * map 合并
      */
//     val map1 = Map[String,Int]("a"->1,"b"->2,"c"->3,("d",4))
//     val map2 = Map[String,Int]("a"->100,"b"->200,"c"->300,("e",500))
//
//    val result: Int = map1.count(tp=>{tp._2>=2})
//    println(result)

//    val resultMap: Map[String, Int] = map1.++:(map2)
//    val resultMap: Map[String, Int] = map1.++(map2)
//    resultMap.foreach(println)

    /**
      * 可变长Map
      */
//    val map = scala.collection.mutable.Map[String,Int]()
//    map.put("zhangsan",100)
//    map.put("lisi",200)
//    map.foreach(println)

  }
}
