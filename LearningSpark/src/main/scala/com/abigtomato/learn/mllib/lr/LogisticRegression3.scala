package com.abigtomato.spark.scala.mllib.lr

import org.apache.spark.mllib.classification.LogisticRegressionWithLBFGS
import org.apache.spark.mllib.linalg
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
/**
 *  线性不可分 ----升高维度
 */
object LogisticRegression3 {

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("spark").setMaster("local[3]")
    val sc = new SparkContext(conf)
    // 解决线性不可分我们来升维,升维有代价,计算复杂度变大了
    val inputData :RDD[LabeledPoint]= MLUtils.loadLibSVMFile(sc, "线性不可分数据集.txt")
      .map { labelpoint =>
        val label = labelpoint.label
        val feature = labelpoint.features
        //新维度的值，必须基于已有的维度值的基础上，经过一系列的数学变换得来
        val array = Array(feature(0), feature(1), feature(0) * feature(1))
        val convertFeature: linalg.Vector = Vectors.dense(array)
        new LabeledPoint(label, convertFeature)
      }
    val splits = inputData.randomSplit(Array(0.7, 0.3),11L)
    val (trainingData, testData) = (splits(0), splits(1))
    val lr = new LogisticRegressionWithLBFGS()
    lr.setIntercept(true)
    val model = lr.run(trainingData)
    val result = testData.map {point => Math.abs(point.label - model.predict(point.features)) }
    println("正确率=" + (1.0 - result.mean()))
    println(model.weights.toArray.mkString(" "))
    println(model.intercept)

  }
}
