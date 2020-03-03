package com.abigtomato.spark.scala.mllib.lr

import org.apache.spark.mllib.classification.LogisticRegressionWithLBFGS
import org.apache.spark.mllib.optimization.{L1Updater, SquaredL2Updater}
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.{SparkConf, SparkContext}
/**
 * 鲁棒性调优
 * 提高模型抗干扰能力
 */
object LogisticRegression5 {

  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("spark").setMaster("local[3]")
    val sc = new SparkContext(conf)
    val inputData = MLUtils.loadLibSVMFile(sc, "健康状况训练集.txt")
    val splits = inputData.randomSplit(Array(0.8, 0.2),100L)
    val (trainingData, testData) = (splits(0), splits(1))
    /**
     * LogisticRegressionWithSGD 既有L1 又有L2正则化(默认)
     */
//    val lr = new LogisticRegressionWithSGD()
//    lr.setIntercept(true)
////    lr.optimizer.setUpdater(new L1Updater())
//    lr.optimizer.setUpdater(new SquaredL2Updater)
    
    /**
     * LogisticRegressionWithLBFGS 既有L1 又有L2正则化(默认)
     */
    val lr = new LogisticRegressionWithLBFGS()
    lr.setNumClasses(2)
    lr.setIntercept(true)
//    lr.optimizer.setUpdater(new L1Updater)
    lr.optimizer.setUpdater(new SquaredL2Updater)
    
    /**
     *  这块设置的是我们的lambda,越大越看重这个模型的推广能力,一般不会超过1,0.4是个比较好的值
     */
    lr.optimizer.setRegParam(0.2)
    val model = lr.run(trainingData)
    val result=testData.map{point=>Math.abs(point.label-model.predict(point.features)) }
    println("正确率="+(1.0-result.mean()))
    println(model.weights.toArray.mkString(" "))
    println(model.intercept)

  }
}
