package com.abigtomato.spark.scala.core.persist

import org.apache.spark.{SparkConf, SparkContext}

/**
  * checkpoint：
  *   - 当RDD的lineage比较长，计算较为复杂时，可以使用checkpoint对RDD进行持久化，checkpoint将数据直接持久化到磁盘中。
  * checkpoint执行流程：
  *   - 1.当spark job执行完之后会从后往前回溯，对执行checkpoint的RDD进行标记；
  *   - 2.回溯完成之后，Spark框架会启动一个job重新计算出checkpoint RDD的数据；
  *   - 3.计算完成之后，将计算的结果直接持久化到指定的checkpoint目录中，切断RDD之间的依赖关系。
  * 优化：
  *   - 对RDD进行checkpoint之前先对RDD进行cache()，这样第三步就不用重新从头计算当前checkpoint RDD的数据。
  */
object Checkpoint {
  def main(args: Array[String]): Unit = {
    val conf =new SparkConf()
      .setAppName("checkpoint")
      .setMaster("local")
    val sc = new SparkContext(conf)

    val lines = sc.textFile("./data/words")
    val lines2 = sc.textFile("./data/words")

    // 设置检查点目录
    sc.setCheckpointDir("./data/checkpoint")

    // 指定RDD设置检查点
    lines.checkpoint()
    lines2.checkpoint()

    // 获取检查点目录
    println(lines2.getCheckpointFile)

    sc.stop()
  }
}
