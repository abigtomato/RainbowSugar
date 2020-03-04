package com.abigtomato.learn.basic

import scala.io.StdIn

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
 */
object Basic {

  /**
    * 编译器的逃逸分析：
    *   1. 如果一个变量生命周期长，被多方引用，编译器会动态的将该变量存入堆空间
    *   2. 如果一个变量只是临时的，编译器只会将其放入栈空间
    */
  def main(args: Array[String]): Unit = {
    /**
      * val为常量，var为变量：
      *   1. 实际编程中，创建变量并指向对象后，很少会再去修改变量指向的对象，此时使用val更合适
      *   2. val没有线程安全的优化，因此效率更高，设计者推荐使用val
      *   3. 如果变量需要改变，则使用var，如：对象中的字段属性
      */
    val str1 = "Java" // 类型推导
    val str2: String = "Scala"
    // +号两边有一边出现字符串则会做拼接操作，都是数值类型则为计算操作
    println(str1 + str2)
    println("类型判断: ", str1.isInstanceOf[String])

    /**
      * 数据类型：
      *   1. 有一个根类型Any，是所有类的父类
      *   2. 没有基本类型，只有对象类型，分为2大类：AnyVal值类型和AnyRef引用类型
      *   3. Null类型是所有AnyRef的子类型，只能取一个值：null
      *   4. Nothing类型是所有类型的子类型，可以将Nothing的值赋给任意类型的变量和函数
      */
    val bo: Boolean = false // 占用1个字节（8bit），取整true或false
    val by: Byte = 127  // 8位有符号补码整数
    val ch: Char = '中'  // 16位无符号Unicode字符
    val sh: Short = 32767 // 16位有符号补码整数
    val in: Int = 2147483647 // 32位有符号补码整数
    val lo: Long = 9223372036854775807L  // 64位有符号补码整数
    val fl: Float = 10.67F // 32位单精度浮点数，低精度的值可以向高精度隐式转换
    val dou: Double  = 180.15 // 64位双精度浮点数
    val str: String = "Albert"  // 字符序列
    def sayUnit(): Unit = {}  // Unit等价于java的void，只有一个值：()
    val nus: String = null  // Null类型只有一个实例：null，可以赋值给任意引用类型（AnyRef），不能赋给值类型（AnyVal）
    def sayHello: Nothing = throw new Exception("异常") // Nothing可以当做抛出异常的函数的返回值，因为是所有类型的子类，所以可以返回任意类型的异常（Nothing可以赋给任意变量）
    printf("Name=%s, Age=%d, sal=%.2f, hei=%.3f\n", str, in, fl, dou)  // 格式化输出
    println(s"Name=$str, Age=$in, Score=$sh, Sal=$fl, Hei=$dou")  // 解析变量/表达式输出
    println(s"Name=${"god:" + str}, Age=${10 + in}, sal=${fl + 8000.0}, hei=${dou + 10}")
    println("sayUnit: " + sayUnit)

    /**
      * 自动类型转换：小转大
      */
    // 1. 有多种类型混合运算时，系统会将所有类型转换为容量最大的类型再进行计算
    val num1 = 10
    val num2 = 20f
    print((num1 + num2).isInstanceOf[Double])
    // 2. byte，short，char不会自动转换，三者在进行计算时会转换为int类型
    val byt: Byte = 10
    val sht: Short = 20
    val cht: Char = 'a'
    println((byt + sht + cht).isInstanceOf[Int])

    /**
      * 强制类型转换：大转小
      */
    val n1: Int = 10 * 3.5.toInt + 6 * 1.5.toInt  // 36
    val n2: Int = (10 * 3.5 + 6 * 1.5).toInt  // 44
    println(n1 + "\t" + n2)

    /**
      * 面试题：不使用中间变量交换啊a，b的值
      */
    var a = 10
    var b = 20
    a = a + b
    b = a - b
    a = a - b
    println(a + "\t" + b)

    /*
      标准输入
     */
    println("StdIn: " + StdIn.readLine())
  }
}