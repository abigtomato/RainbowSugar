package com.abigtomato.learn.basic

/**
 * 1.object 表示伴生对象
 * 2.HelloWorld 表示伴生对象名，底层真正的类名是HelloWorld$
 * 3.使用scalac编译object HelloWorld后底层会生成2个.class文件，分别为HelloScala，HelloScala$
 * 4.scala运行时的流程：
 *  - a.从HelloWorld的main开始
 *   public final class HelloScala {
 *     public static void main(String[] paramArrayOfString) {
 *       HelloScala$.MODULE$.main(paramArrayOfString);
 *     }
 *   }
 *  - b.调用的是HelloWorld$类的方法：HelloScala$.MODULE$.main
 *   public final class HelloScala$ {
 *     public static final MODULE$;
 *
 *     static {
 *       new()
 *     }
 *
 *     public void main(String[] args){
 *       Predef..MODULE$.println("Hello Scala");
 *     }
 *
 *     private HelloScala$() { MODULE$ = this; }
 *   }
 */
object HelloWorld {
  /*
   * 1.def 声明方法
   * 2.main 程序入口
   * 3.args: Array[String] 形参
   * 4.Unit 返回值
   * 5.函数声明 = 函数体
   */
  def main(args: Array[String]): Unit = {
    println("Hello Scala")
  }
}