package com.abigtomato.learn.wc

import org.apache.flink.streaming.api.scala._

/**
 * 无界流WordCount
 */
object StreamWordCount {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val dataStream: DataStream[String] = env.socketTextStream("localhost", 7777)

    val wordCountDataStream = dataStream.flatMap(_.split(" "))
      .filter(_.nonEmpty)
      .map((_, 1))
      .keyBy(0)
      .sum(1)
    wordCountDataStream.print()

    env.execute("stream wordcount job")
  }
}
