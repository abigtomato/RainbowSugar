package com.abigtomato.learn.wc

import org.apache.flink.api.scala._

/**
 * 有界流WordCount
 */
object WordCount {

  def main(args: Array[String]): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment

    val inputPath = "src/main/resources/data.csv"
    val inputDataSet: DataSet[String] = env.readTextFile(inputPath)

    val wordCountDataSet = inputDataSet
      .flatMap(data => data.split(" "))
      .map((_, 1))
      .groupBy(0)
      .sum(1)
    wordCountDataSet.print()
  }
}
