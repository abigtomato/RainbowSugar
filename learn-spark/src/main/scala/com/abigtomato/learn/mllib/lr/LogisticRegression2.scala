package com.abigtomato.spark.scala.mllib.lr

import org.apache.spark.mllib.classification.LogisticRegressionWithSGD
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 有无截距
  */
object LogisticRegression2 {

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("spark").setMaster("local[3]")
    val sc = new SparkContext(conf)
    val inputData: RDD[LabeledPoint] = MLUtils.loadLibSVMFile(sc, "w0测试数据.txt")
    /**
     * randomSplit(Array(0.7, 0.3))方法就是将一个RDD拆分成N个RDD，N = Array.length
     * 第一个RDD中的数据量和数组中的第一个元素值相关
     */
    val splits = inputData.randomSplit(Array(0.7, 0.3),11L)
    val (trainingData, testData) = (splits(0), splits(1))
    val lr = new LogisticRegressionWithSGD
    // 设置要有W0，也就是有截距
    lr.setIntercept(true)
    val model=lr.run(trainingData)
    val result=testData.map{labeledpoint=>Math.abs(labeledpoint.label-model.predict(labeledpoint.features)) }
    println("正确率="+(1.0-result.mean()))
    println(model.weights.toArray.mkString(" "))
    println(s"intercept = ${model.intercept}")
  }
}
