package lesson

/**
  * match注意：
  * 1.match 不仅可以匹配值还可以匹配类型
  * 2. case _=>{"no ... match ...."} 代表默认匹配，什么都匹配不上再匹配，放在匹配最后。
  * 3.匹配过程中会有数据类型转换
  * 4.从上往下依次匹配，匹配上之后，就自动终止
  * 5.xx match {xxx} 相当于是一整行，方法的{...}可以省略
  */
object Lesson_match {
  def main(args: Array[String]): Unit = {
    val tuple = (1,"hello",1.1,true,'c',2.5f)

    val iter = tuple.productIterator
//    iter.foreach(x=>{MatchTest(x)})
    iter.foreach(MatchTest)

  }

  def MatchTest(o:Any)=
    o match {
      case i:Int=>{println(s"type is Int ,value is $i")}
      case 1=>{println(s"value is 1")}
      case s:String =>{println(s"type is String, value is $s")}
      case d:Double =>{println(s"type is Double ,value is $d")}
      case 'c'=>{println("value is c")}
      case _=>{println("no ... match ....")}
    }

}
