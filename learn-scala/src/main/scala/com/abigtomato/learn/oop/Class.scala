package com.abigtomato.learn.oop

import scala.beans.BeanProperty
import scala.collection.mutable

/*
    类：[修饰符] class 类名(主构造器的参数列表)
    修饰符：默认public（scala中一个文件可定义多个public的类）
    主构造器：
    1. 主构造器底层会编译成一个单独的方法，方法参数就是主构造器的参数
    2. 主构造器第一行隐式调用super()（父类主构造器）
    3. 主构造器的修饰符直接在参数列表前添加即可
    4. 类中的一切非属性非方法定义的语句，都会在编译时添加到主构造器中
    5. 如果主构造器的参数没有任何声明，该形参就是主构造器的局部变量
        如果使用val修饰形参，那么该参数就是类的私有只读属性private final 参数名
        如果使用var修饰形参，那么该参数就是类的成员属性了
 */
class Cat private(inName: String, val inAge: Int) {
  /*
      成员属性：[修饰符] var 属性名[: 类型] = 属性值
      1. 若不加访问控制符定义属性，底层自动置为private前缀
      2. 同时会生成两个public方法：
          属性名() 相当于 getter
          属性名_$eq(val) 相当于 setter
      3. 若显示添加private的访问控制符，那么自动生成的方法也是private的
   */
  var name: String = inName
  var age: Int = inAge
  // @BeanProperty：引入JavaBeans规范的getXXX()，setXXX()方法
  @BeanProperty var color: String = _   // _表示默认值

  // 类中的一切非属性非方法定义的语句，都会在编译时添加到主构造器中
  println("~~~~~~~~~~~~~~~")

  /*
      辅助构造器：def this(参数列表)
      第一行必须显示的调用主构造器
   */
  def this(inName: String, inAge: Int, inColor: String) {
    this(inName, inAge)
    this.color = inColor
  }

  /*
      overrider：声明方法重写
      def toString：类的成员方法定义（成员方法默认public修饰）
   */
  override def toString: String = this.name + "\t" + this.age + "\t" + this.color
  def sayCat(): Unit = println(this.name + "\t" + this.age + "\t" + this.color)
}

/*
    伴生对象：
    1. 一个类可以有一个和自身同名的伴生对象，里面定义静态属性和方法
    2. 底层会被编译成：伴生对象名$.class 文件存在
    3. 调用静态方法或属性：伴生对象名.方法/属性（底层通过 伴生对象名.MODULE$.sayHi() 格式调用，MODULE$表示this）
 */
object Cat {
  def sayHi(): Unit = println("object cat hello")
  // 当使用者通过 伴生对象名() 的方式生成对象时，会自动调用对应的apply方法
  def apply(): Cat = new Cat("unit", 3, "block")
  def apply(name: String, age: Int, color: String): Cat = new Cat(name, age, color)
}

/*
    类的继承
 */
//class SiameseCat extends Cat {
//  // 方法重写
//  override def sayCat(): Unit = {
//      println("SiameseCat: ")
//      // 调用父类
//      super.sayCat()
//  }
//}

/*
    抽象类：
    1. 抽象属性和方法没有初值或方法体
    2. 既可以定义抽象方法，也可以有实际的方法
 */
abstract class Animal {
  val name: String
  def say(): Unit
  def play(): Unit = println("abstract play")
}

/**
 * case class 注意：
 *  - 1.样例类中的参数就是当前类的属性，默认有getter，setter方法（定义时指定成var）
 *  - 2.样例类实现了toString,equals，copy和hashCode等方法。
 *  - 3.样例类可以new 可以不new
 */
case class Human(var name: String, age: Int)

// object定义静态方法和静态字段
object Class {
  def main(args: Array[String]) {
    /*
        对象：val|var 对象名[: 类] = new 类()
        1. 内存：引用入栈，类的属性存入堆区，方法存入方法区
        2. 创建流程：
            2.1 加载类的信息（属性，方法）
            2.2 开堆内存中开辟空间
            2.3 使用父类的构造器
            2.4 使用主构造器对属性初始化
            2.5 使用辅助构造器进行初始化
            2.6 将开辟的对象空间地址赋值给引用变量
     */
    val cat = new Cat("ll", 3, "blue")
    cat.name = "lily"   // 底层调用cat.name_$eq("lily")
    println(cat.name)   // 底层调用cat.name()
    println(cat.inAge)

    // 引入包，改别名
    import java.util.{ HashMap => JavaHashMap, List }
    var map = new mutable.HashMap()
    var jmap = new JavaHashMap()

    // 继承关系测试
//    val siam = new SiameseCat
//    siam.name = "jack"  // 调用了从父类继承的共有的name_eq$()方法
//    println(siam.name)  // 调用了从父类继承的共有的name()方法
//
//    println(classOf[String])    // 获取对象的类名
//    println("Scala".isInstanceOf[String])   // 判断某个对象是否属于某个类
//    cat.asInstanceOf[SiameseCat]    // 将引用转换为子类的引用

    // 匿名子类测试
    val animal = new Animal {
      override val name: String = ""

      override def say(): Unit = {
        println("Animal OK")
      }
    }

    // 调用伴生对象的静态方法
    Cat.sayHi()

    // 测试apply
    val cat1 = Cat
    val cat2 = Cat("nini", 5, "red")
    cat2.sayCat()

    // 测试样例类
    val h1 = Human("zhangsan", 18)
    val h2 = Human("zhangsan", 18)
    println(h1 == h2)
  }
}