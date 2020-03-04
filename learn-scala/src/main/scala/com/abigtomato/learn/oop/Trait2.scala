package com.abigtomato.learn.oop

/*
    特质：
    1. 既可以定义抽象方法，又可实现具体方法
 */
trait Logger {
  def log(msg: String)
  def info(msg: String): Unit = log("INFO: " + msg)
  def warn(msg: String): Unit = log("WARN: " + msg)
  def severe(msg: String): Unit = log("SEVERE" + msg)
}

// 继承Logger，实现抽象方法的特质
trait ConsoleLogger extends Logger {
  def sayHello(): Unit
  def log(msg: String): Unit = println(msg)
}

trait TimestampLogger extends ConsoleLogger {
  override def log(msg: String): Unit = super.log(new java.util.Date() + " " + msg)
}

trait ShortLogger extends ConsoleLogger {
  val maxLength = 15  // 初始化就是具体字段
  val minLength: Int  // 不初始化就是抽象字段（在子类中必须被重写）
  override def log(msg: String): Unit = super.log(if(msg.length <= 15) msg else s"${msg.substring(0, 12)}...")
}

class Account {
  protected var balance = 0.0
}

abstract class SavingsAccount extends Account with Logger {
  def withdraw(amount: Double) {
    if(amount > balance) log("Insufficient funds")
    else balance -= amount
  }
}

// 通过extends继承特质，可以通过with继承多个特质
class SerLogger extends Logger with Cloneable with Serializable {
  def log(msg: String): Unit = println(msg)
}

object Trait {
  def main(args: Array[String]) {
    val ser = new SerLogger
    ser.log("Hello Scala")

    /*
        动态混入对象：
        1. 构建对象时动态混入一个或多个特质
        2. 该对象可以直接使用特质具体实现的方法和属性
        3. 特质中的抽象方法或属性需要重写或赋值
     */
    val acct = new SavingsAccount with ConsoleLogger {
      override def sayHello(): Unit = println("trait sayHello")
    }
    acct.withdraw(100)
//    println(acct.maxLength)

    /*
        叠加特质：
        1. 特质with的顺序从左到右
        2. 执行叠加特质的方法时，会从右向左开始执行
        3. 特质内部如果调用super，是向左边特质查找，找不到才会到父特质查找
        4. 如果想调用父特质的方法，使用super[父特质].xxx()...
     */
    val acct1 = new SavingsAccount with TimestampLogger with ShortLogger {
      val minLength = 20

      override def sayHello(): Unit = ???
    }
    val acct2 = new SavingsAccount with ShortLogger with TimestampLogger {
      override val minLength: Int = 0

      override def sayHello(): Unit = ???
    }
    acct1.withdraw(100)
    acct2.withdraw(100)
  }
}