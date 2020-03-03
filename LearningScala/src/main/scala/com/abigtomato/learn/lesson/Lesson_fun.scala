package lesson

object Lesson_fun {
  def main(args: Array[String]): Unit = {
    /**
      * 1.方法定义
      *   注意：
      *     1).Scala中方法的定义使用def ,方法的参数一定要指明类型
      *     2).方法体的return可以省略，Scala方法自动将方法体中最后一行计算的结果当做返回值返回。
      *       如果使用return,方法体的返回值类型要显式的声明。
      *     3).方法体的返回值类型可以省略，Scala可以自动推断，如果省略了返回值类型，不能使用return。
      *     4).Scala中方法体中如果可以使用一行搞定，方法体的“{...}”可以省略。
      *     5).Scala定义方法如果将“=”省略不写，无论方法最后一行计算的结果是什么，都会被丢弃，返回unit
      *
      */
//    def max(x:Int,y:Int) {
//      if(x>y){
//        x
//      }else{
//        y
//      }
//    }
//    println(max(10,20))

//    def max(x:Int,y:Int)= if(x>y) x else y
//
//    println(max(10,20))

//    def max(x:Int,y:Int)= {
//      if(x>y){
//        x
//      }else{
//        y
//      }
//    }
//
//    val result = max(10,20)
//    println(result)

    /**
      * 2.递归方法
      *   递归方法中需要显示的声明方法体的返回值类型
      */
//    def fun(a:Int):Int = {
//      if(a==1){
//        a
//      }else{
//        a * fun(a-1)
//      }
//    }
//    println(fun(5))

    /**
      * 3.方法的参数有默认值
      */
//    def fun(a:Int=10,b:Int=100) = {
//      a+b
//    }
//
//    println(fun(a=20))

    /**
      * 4.可变长参数的方法
      */

//    def fun(s:String*) = {
//      s.foreach(println)

//      s.foreach(println(_))

//      s.foreach(elem=>{
//        println(elem)
//      })
//      s.foreach((elem:String)=>{
//        println(elem)
//      })
//    }

//    fun("hello world ","a","b","c","d")

//    def funn(s:String*) = {
//      s.foreach(s=>{println(s)})
//      for(elem <- s){
//        println(elem)
//      }
//    }
//
//    fun("hello world ","a","b","c","d")


    /**
    *  5. 匿名函数
    *   注意：=> 就是匿名函数
    */
//    val fun = ()=>{
//      println("hello world")
//    }
//
//    fun()

//    val fun: (Int, Int) => Int = (a:Int,b:Int) => {
//      a+b
//    }
//    println(fun(10,20))


    /**
      * 6.嵌套方法
      */
//    def fun(a:Int) = {
//        def fun1(num :Int):Int ={
//          if(num==1){
//            num
//          }else{
//            num * fun1(num-1)
//          }
//        }
//      fun1(a)
//    }
//    println(fun(5))

    /**
      * 7.偏应用表达式
      */

//    def showLog(d:Date,log:String)  ={
//      println(s"date is $d ,log is $log")
//    }
//
//    val dd = new Date()
//
//    showLog(dd,"a")
//    showLog(dd,"b")
//    showLog(dd,"c")
//
//   val fun = showLog(dd,_:String)
//    fun("aaa")
//    fun("bbb")
//    fun("ccc")

    /**
      * 8.高阶函数
      *  1).方法参数是函数
      *  2).方法的返回是函数
      *  3).方法的参数和返回都是函数
      *
      */
    //方法的参数是函数
//    def fun(a:Int,b:Int):Int ={
//      a+b
//    }
//
//    def fun1(f:(Int,Int)=>Int,s:String) = {
//      val r: Int = f(10,20)
//      r + "#" + s
//    }
//
//    val str: String = fun1((v1:Int,v2:Int)=>{v1 * v2},"hello")
//    println(str) //200#hello

    //方法的返回是函数
//    def fun(a:Int,b:Int):(String,String)=>String = {
//      val r = a+b
//      def fun1(s1:String,s2:String):String ={
//        s1+"#"+s2 +"$"+r
//      }
//      fun1
//    }
//
//    val str: String = fun(10,20)("a","b")
//    println(str) //a#b$30

    //方法的参数和返回都是函数
//    def fun(f:(Int,Int)=>Int,s1:String):(String,String)=>String ={
//      val r = f(1,2)
//      val p = r + "$" + s1
//      def fun1(ss1:String,ss2:String):String = {
//        p +"@"+ss1+"#"+ss2
//      }
//      fun1
//    }
//
//   val result  =  fun((a:Int,b:Int)=>{a+b},"hello")("a","b")
//    println(result) //3$hello@a#b


    /**
      * 9.柯里化函数
      *  高阶函数简化版本
      */
    def fun(a:Int,b:Int)(c:Int,d:Int)= {
      a+b+c+d
    }

    println(fun(1,2)(3,4))












  }
}
