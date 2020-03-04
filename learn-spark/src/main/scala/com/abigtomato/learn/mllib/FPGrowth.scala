package com.abigtomato.spark.scala.mllib

import org.apache.spark.mllib.fpm.FPGrowth
import org.apache.spark.{SparkConf, SparkContext}

object FPGrowth {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("FPGrowth").setMaster("local[3]")
    val sc = new SparkContext(conf)

    // 创建交易样本
    val path = "D:\\WorkSpace\\learn.scala\\LearnSpark\\src\\main\\resources\\fpgrowth.txt"
    val transactions = sc.textFile(path).map(_.split(" ")).cache()
    println(s"交易样本的数量为: ${transactions.count()}")

    // 最小支持度
    val minSupport = 0.4
    // 计算并行度
    val numPartition = 2
    // 模型训练
    val model= new FPGrowth()
      .setMinSupport(minSupport)
      .setNumPartitions(numPartition)
      .run(transactions)
    println(s"经常一起购买的物品集数量为: ${model.freqItemsets.count()}")

    // 获取构建结果
    model.freqItemsets.collect.foreach(itemset => {
      println(itemset.items.mkString("[", ",", "]") + "," + itemset.freq)
    })

    sc.stop()
  }
}
