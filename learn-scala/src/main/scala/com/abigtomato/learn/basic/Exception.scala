package com.abigtomato.learn.basic

object Exception {
  def main(args: Array[String]) {
    // throw表达式抛出异常，表达式类型是Nothing，所以可以用在任何地方（比如下面的代码，返回值是Int，但由于Nothing是Int的子类，所以也可以匹配）
    def divide(x: Int, y: Int): Int = if(y == 0) throw new ArithmeticException("Divide By Zero") else x / y

    // 捕获异常
    try {
        divide(1, 0)
    } catch {
        // 匹配异常
        case ex: ArithmeticException => println(ex.getMessage)  // 越具体的异常越靠前
        case ex: Exception => println(ex.getMessage)
    } finally {
        println("finally")
    }

    // throws向调用者声明此函数可以引发该异常，有助于调用函数用try-catch处理
    @throws(classOf[NumberFormatException])
    def func() = "abc".toInt
    try {
        func()
    } catch {
        case ex: NumberFormatException => println(ex.getMessage)
    }
  }
}