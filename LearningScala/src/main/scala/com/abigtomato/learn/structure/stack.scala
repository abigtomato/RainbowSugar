package structure

import scala.collection.mutable.Stack

object Learn {
    def main(args: Array[String]) {
        // 堆栈
        val s = new Stack[Int]

        s.push(1, 2, 3) // 压栈
        println(s)

        println(s.pop()) // 弹栈
        println(s.top)  // 查看栈顶
    }
}