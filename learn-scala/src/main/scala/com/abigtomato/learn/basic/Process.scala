package com.abigtomato.learn.basic

import scala.util.control.Breaks.{break, breakable}

object Process {
  def main(args: Array[String]): Unit = {
    /*
        分支控制
     */
    val age = 20
    // if表达式会返回一个值，如果if...else...返回类型不同，整个表达式就返回Any类型
    val s1 = if(age > 0) "Scala" else -1
    println(s1)

    // 如果判断语句什么都没有返回，表达式会默认返回Unit类型（或者手写"()"）
    val s2 = if(age < 0) "Scala" else ()
    println(s2)

    // 多层结构（最好不超过3层）
    if(age < 18) {
        println("666")
    } else if(age >= 18 && age <= 20) {
        println("233")
    } else {
        println("555")
    }

    /*
        while循环控制
     */
    var index = 0
    // 整个while表达式的值为Unit类型
    val ret = while (index < 100) {
        index += 1
    }
    println("\n" + ret)
    /*
        breakable对break()抛出的异常做了处理，使接下来的代码可以继续执行
        源码：
        def breakable(op: => Unit) {
            try {
                op
            } catch {
                case ex: BreakControl =>
                    if (ex ne breakException) throw ex
            }
        }
     */
    breakable {
        do {
            index -= 1
            if (index == 18) {
                break()
            }
        } while (index > 100)
    }

    /*
        for循环控制
     */
    // 1. 推导式，"变量名 <- 集合"也称为生成器表达式（表达式基于集合生成单独的数值）
    for(elem <- List("Java", "Python", "Scala", "Golang")) print(s"${elem}\t")
    println()

    // 2. 保护式（守卫），就是添加if判断
    for(i <- 1 until 10; if i != 5) print(s"${i}\t")
    println()

    // 3. 循环引入变量
    for(i <- 1 to 3; from = 4 - i) print(s"${from}\t")
    println()

    // 4. 循环嵌套
    for(i <- 1 to 3; if i > 0; j <- 1 to 3; if i != j) print(s"${10 * i + j}\t")
    println()

    // 4. 循环返回值
    val list = for(i <- 1 to 10) yield { if(i % 2 == 0) i else () }  // 循环的结果赋予列表
    for(elem <- list) print(s"${elem}\t")
    println()

    // 5. for{} 写法
    for {
        i <- 1 to 3
        from = 4 - i
        j <- from to 3
    } print(s"${10 * i + j}\t")
    println()

    // 6. 步长控制
    /*
        val Range = scala.collection.immutable.Range
        object Range {
            ......
            def apply(start: Int, end: Int, step: Int): Range = new Range(start, end, step)
            ......
        }
    */
    for(i <- Range(1, 10, 2)) print(s"$i\t")
    println()

    // for练习：99乘法表
    for (i <- 1 to 9) {
        for (j <- 1 to i) {
            print(s"$j * $i = ${i * j}\t")
        }
        println()
    }

    // for练习：金字塔
    val num = 6
    for (i <- 1 to num) {   // 控制层数
        for (j <- 1 to num - i) {   // 控制空格，规律：空格数 = 总层数 - 当前层数
            print(" ")
        }
        for (k <- 1 to i + (i - 1)) {   // 控制*号，规律：*号数 = 当前层数 + (当前层数 - 1)
            print("*")
        }
        println()
    }
  }
}