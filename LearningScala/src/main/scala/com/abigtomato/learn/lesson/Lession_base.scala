package lesson

/**
  * Scala 注意问题：
  * 1. Scala语言中每行后面有分号推断机制。
  * 2. Scala中定义常量使用val 不可变，定义变量使用var，变量可变
  * 3. Scala中有类型自动推断机制，可以自动推断变量和常量的类型。
  * 4. Scala 类中的属性自带getter，setter方法【属性必须是var】,类可以传参，传参就有了默认的构造。
  *    重写构造后，构造方法中第一行需要先调用默认构造,当new 类时，类中除了方法不执行【不含对应的构造方法】，其他都执行。
  *    同一包下或者同一个文件中不能有同名的class
  * 5. object 相当于java中的单例对象，object中定义的全是静态。同一包下或者同一个文件中不能有同名的object,Object不可以传参
  *   实现object类似传参的功能可以使用object中apply方法。
  * 6. Scala中定义方法、属性、类、对象时建议符合驼峰命名法。
  * 7. 在一个Scala文件中如果类和对象的名称一致，那么这个类叫做这个对象的伴生类，这个对象叫做这个类的伴生对象，他们之间可以访问私有的变量。
  *
  */

class Person(xname:String,xage:Int){
  private var name = xname
  val age = xage
  var gender = 'm'

  def this(yname:String,yage:Int,ygender:Char){
    this(yname,yage)
    this.gender = ygender
  }
//  println("**********************")

//  def showScore() = {
//    println(s"score = ${lesson.Lession_base.score}")
//  }

}


object Lession_base {
//  println("==================")
//  val score = 100

//  def apply(s:String,a:Int) = {
//    println(s" s = $s,a = $a")
//  }
//  def apply(s1:String) = {
//    println(s" s = $s1")
//  }

  def main(args: Array[String]): Unit = {
    /**
      * while ..   | do ...while
      */
    var i = 0
    do{
      println(s"第 $i 次求婚。。。")
            i +=1
    }while(i<10)
//    while(i<10){
//      println(s"第 $i 次求婚。。。")
//      i +=1
////      i =i+1
//    }

    /**
      * for...
      * 1 to 10 操作符操作
      */
//    val result = for(i <- 1 to 100 if(i%5==0) if(i>50)) yield i
//    println(result)

//    for(i <- 1 to 100 if(i%5==0) if(i>50)){
//      println(i)
//    }
//    for(i <- 1 until 10 ; j <- 1 until 10){
//      println(s" i = $i , j = $j")
//    }

//    println(1.to(10,2))
//    println(1 until (10,3))
//    for(i <- 1 until 10){
//      for(j <- 1 until 10){
//        println(s" i = $i , j = $j")
//      }
//    }

    /**
      * if ... else ...
      */
//    var age = 20
//    if(age<18){
//      println("age < 18")
//    }else if (age>=18&&age<=30){
//      println("age>=18&&age<=30")
//
//    }else{
//      println("age > 30")
//    }




//    lesson.Lession_base("zhangsan",100)
//    lesson.Lession_base("xxxx")

//    val p = new lesson.Person("lisi",20)
//    p.showScore()


//    val p1 = new lesson.Person("diaochan",18,'f')
//    println(s"name = ${p1.name} ,gender = ${p1.gender}")
//
//    val p2 = new lesson.Person("zhangsan",18)
//    println("name = "+p2.name +",gender = "+p2.gender)

//    val person = new lesson.Person()
//    person.name = "lisi"
//    println(person.name)
//    println(person.age)


//    println("hello world")
//    val a = 100
//    var b = 20
//    b =1000
//    println(a + b)
  }
}
