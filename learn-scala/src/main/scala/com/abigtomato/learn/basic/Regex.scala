package com.abigtomato.learn.basic

object Regex {
  def main(args: Array[String]): Unit = {
    // 字符串.r转换为正则表达式
    val numPattern = "[0-9]+".r
    val wsnumwsPattern = """\s+[0-9]+\s+""".r

    // 匹配所有
    for(matchString <- numPattern.findAllIn("99 bottles, 98 bottles")) print(matchString + " ")
    println()

    // 匹配结果转换为数组
    val matches = numPattern.findAllIn("99 bottles, 98 bottles").toArray
    matches.foreach(elem => print(elem + " "))
    println()

    // 从头开始匹配一个
    val m1 = wsnumwsPattern.findFirstIn("99 bottles, 98 bottles")
    println(m1)

    // 替换
    val str1 = numPattern.replaceFirstIn("99 bottles, 98 bottles", "XX")
    val str2 = numPattern.replaceAllIn("99 bottles, 98 bottles", "XX")
    println(str1 + " | " + str2)

    // 取括号组
    val numitemPattern = "([0-9]+) ([a-z]+)".r
    val numitemPattern(num, item) = "99 bottles"
    println(num, item)
    for(numitemPattern(num, item) <- numitemPattern.findAllIn("99 bottles, 98 bottles")) println(num, item)
  }
}
