package com.abigtomato.spark.scala.mllib.lr

import org.apache.spark.mllib.classification.LogisticRegressionWithLBFGS
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.{SparkConf, SparkContext}
/**
 * 设置分类阈值
 */

object LogisticRegression4 {

  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("spark").setMaster("local[3]")
    val sc = new SparkContext(conf)
    /**
     * LabeledPoint = Vector+Y
     */
    val inputData = MLUtils.loadLibSVMFile(sc, "健康状况训练集.txt")
    val splits = inputData.randomSplit(Array(0.7, 0.3),11L)
    val (trainingData, testData) = (splits(0), splits(1))
    val lr = new LogisticRegressionWithLBFGS()
    lr.setIntercept(true)
    
//    val model = lr.run(trainingData)
//    val result = testData
//      .map{point=>Math.abs(point.label-model.predict(point.features)) }
//    println("正确率="+(1.0-result.mean()))
//    println(model.weights.toArray.mkString(" "))
//    println(model.intercept)
    /**
     * 如果在训练模型的时候没有调用clearThreshold这个方法，那么这个模型预测出来的结果都是分类号
     * 如果在训练模型的时候调用clearThreshold这个方法，那么这个模型预测出来的结果是一个概率
//     */
    val model = lr.run(trainingData).clearThreshold()
    val errorRate = testData.map{p=>
      //score就是一个概率值
      val score = model.predict(p.features)
      // 癌症病人宁愿判断出得癌症也别错过一个得癌症的病人
      val result = score>=0.1 match {case true => 1 ; case false => 0}
      Math.abs(result-p.label)
    }.mean()
    println(s"正确率 = ${1-errorRate}")
  }
}
