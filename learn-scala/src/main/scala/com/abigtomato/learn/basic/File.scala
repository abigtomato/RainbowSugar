package com.abigtomato.learn.basic

import java.io.{FileInputStream, FileOutputStream, ObjectInputStream, ObjectOutputStream, PrintWriter, Serializable}

import scala.io.Source

object File {

  @SerialVersionUID(42L) class Person extends Serializable

  def main(args: Array[String]): Unit = {
    // 按行读取
    def lineRead(): Unit = {
      val source = Source.fromFile("./myfile.txt", "UTF-8")
      val lineIterator = source.getLines
      for(line <- lineIterator) {
        println(line)
      }
      source.close()
    }

    // 转换为数组
    def readToArray(): Unit = {
      val source = Source.fromFile("./myfile.txt", "UTF-8")
      val lines = source.getLines.toArray
      println(lines)
      source.close()
    }

    // 转换为字符串
    def readMkString(): Unit = {
      val source = Source.fromFile("./myfile.txt", "UTF-8")
      val contents = source.mkString(", ")
      println(contents)
      source.close()
    }

    // 按字符读取
    def readChar(): Unit = {
      val source = Source.fromFile("./myfile.txt", "UTF-8")
      val iter = source.buffered
      while (iter.hasNext) {
        println(iter.next)
      }
      source.close()
    }

    // 其他源读取
    val source1 = Source.fromURL("http://www.baidu.com", "UTF-8")
    val source2 = Source.fromString("Hello World!")
    val source3 = Source.stdin

    // 写入文件
    val out = new PrintWriter("./number.txt")
    for(i <- 1 to 100) out.println(i)
    out.close()

    val fred = new Person

    // 序列化
    val serOut = new ObjectOutputStream(new FileOutputStream("./person.obj"))
    serOut.writeObject(fred)
    serOut.close()

    // 反序列化
    val in = new ObjectInputStream(new FileInputStream("./person.obj"))
    val savedFred = in.readObject().asInstanceOf[Person]
    in.close()
  }
}
