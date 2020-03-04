package com.abigtomato.learn.basic

import java.util.Date

/**
 * 1.方法定义
 *   注意：
 *     1).Scala中方法的定义使用def ,方法的参数一定要指明类型
 *     2).方法体的return可以省略，Scala方法自动将方法体中最后一行计算的结果当做返回值返回。
 *       如果使用return,方法体的返回值类型要显式的声明。
 *     3).方法体的返回值类型可以省略，Scala可以自动推断，如果省略了返回值类型，不能使用return。
 *     4).Scala中方法体中如果可以使用一行搞定，方法体的“{...}”可以省略。
 *     5).Scala定义方法如果将“=”省略不写，无论方法最后一行计算的结果是什么，都会被丢弃，返回unit
 */
object Function {
  def main(args: Array[String]): Unit = {
    /*
        函数（声明的完整写法）
        1. 调用一个新函数时会在栈空间压入一个新的受保护的函数空间；
        2. 每一个函数的局部变量都是独有的，相互不受影响；
        3. 当一个函数执行完毕要返回值的时候，会遵守谁调用就返回给谁的原则；
        4. 当在函数中调用另外一个函数时，会在当前暂停位置做标记，
           当被调用函数执行完毕弹出栈空间后，会返回原函数标记处继续执行；
        5. 栈内存存在一个栈顶指针，当新函数被调用并在栈内存压入新的空间时，
           栈顶指针上移指向新空间，当新函数执行结束释放空间后，栈顶执行下移指向下面的函数空间。
     */
    def abs(x: Double): Double = { return if (x >= 0) x else -x }
    println(abs(1))

    /*
        递归函数（此处省略返回值声明，返回值类型自动推导）
        1. 递归函数的执行必须要向退出条件逼近，否则会发送无限递归的情况；
        2. 递归函数执行时，每一次调用都会在栈内存中压入新的函数空间，直到触发退出条件时，
           会从栈顶的函数空间开始弹出（同时开始向调用方传递返回值），直到返回到调用递归函数的函数为止。
     */
    def fibonacci(num: Int): Int = if (num == 1 || num == 2) 1 else fibonacci(num - 1) + fibonacci(num - 2)    // 斐波那契
    def factorial(num: Int): Int = if (num == 1) 1 else num * factorial(num - 1) // 阶乘
    def peach(day: Int): Int = if (day == 10) 1 else (peach(day + 1) + 1) * 2   // 猴子每天吃总桃数的一半多一个，吃到第10天还剩一个，求任意天的桃子数量，第9天 = (第10天数量 + 1) * 2 ...
    println(s"斐波那契(5): ${fibonacci(5)}")
    println(s"阶乘(5): ${factorial(3)}")
    println(s"猴子吃桃问题(9): ${peach(9)}")

    /*
        参数默认值（此处省略return关键字，默认以最后一行为返回值）
     */
    def default(str: String, left: String = "[", right: String = "]") = left + str + right
    println(default("Hello, Scala"))    // 从左到右覆盖
    println(default(str = "Hello, Scala", left = "<", right = ">"))

    /*
        函数嵌套
     */
    def sayOk(): Unit = {   // 底层变为伴生对象的成员：private final sayok$1
        println("main sayok")
        def sayOk(): Unit = println("sayok sayok")  // 底层变为伴生对象的成员：private final sayok$2
    }

    /*
        惰性函数（延迟计算场景：不会提前计算，只有当使用的时候才会真正计算，也就是说可以将耗时的计算推送到需要的时候）
     */
    def lsum(n1: Int, n2: Int) = { println("惰性函数lsum()被执行了"); n1 + n2 }
    lazy val res = lsum(10, 20) // 声明lazy的变量会加载函数进缓冲队列，而不会立即执行（lazy不能修饰var）
    lazy val numv = 666 // 声明lazy的普通变量也会延迟分配内存空间，只有使用numv的时候才会分配
    println(s"res: ${res}") // 当变量被实际使用时才会从缓冲队列中出队函数进入执行队列去顺序执行

    // 可变长参数（可变参数的定义必须放在参数列表的最后一个）
    def sum(elems: Int*) = {
        var result = 0
        for(elem <- elems) result += elem
        result
    }
    println(sum(1, 2, 3, 4, 5))

    // 匿名函数（无返回值的函数叫过程）
    val anonymous = (name: String, facePower: Double) => println(name + " : " + facePower)
    anonymous("albert", 100.0)

    // 占位参数
    def logWithDate(date: Date, log: String) = println(date + " : " + log)
    val fun = logWithDate(new Date(), _: String)
    fun("loging")

    // 泛型函数
    def getMiddle[T](a: Array[T]) = a.length / 2
    println(getMiddle(Array("Mary", "Had", "a", "Little", "Lamb")))
    val func = getMiddle[String] _  //

    // 函数做为参数
    def plus(x: Double) = 3 * x
    Array(3.14, 1.42, 2.0).map(plus)

    // 匿名函数
    Array(3.14, 1.42, 2.0).map(x => 3 * x)
    Array(3.14, 1.42, 2.0).map { x => 3 * x }

    // 高阶函数
    def valueAtOneQuarter(func: (Double) => Double) = func(0.25)    // 接收函数作为参数的函数
    def mulBy(factor: Double) = (x: Double) => factor * x   // 返回函数的函数
    println(valueAtOneQuarter(x => x * x))
    println(mulBy(5)(5))

    // 闭包
    def pack(factor: Double) = (x: Double) => factor * x
    val triple = pack(3)    // 返回闭包函数
    val half = pack(0.5)    // 闭包函数由匿名函数和其引用的非局部变量组合而成
    println(triple(14) + " : " + half(14))

    // 柯里化函数
    def mulOneAtATime(x: Int) = (y: Int) => x * y   // 柯里化就是接收多个参数的函数转换为接收单个参数的函数的过程（使用闭包）
    def mulOneAtATime2(x: Int)(y: Int) = x * y  // 简写
    println(mulOneAtATime(7)(8))

    // 控制抽象
    def runInThread(block: () => Unit) {    // 参数是函数，并且函数的参数没有输入值也没有返回值
        new Thread {
            override def run() { block() }
        }.start()
    }
    runInThread { () => println("Hi"); Thread.sleep(1000); println("Bye") }
  }
}