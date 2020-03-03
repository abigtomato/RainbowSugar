package com.abigtomato.spark.scala.mllib.lr

import org.apache.spark.mllib.classification.{LogisticRegressionWithLBFGS}
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.{SparkConf, SparkContext}
/**
 * 逻辑回归 健康状况训练集   
 */
object LogisticRegression {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("spark").setMaster("local[3]")
    val sc = new SparkContext(conf)
    //加载 LIBSVM 格式的数据  这种格式特征前缀要从1开始 
    val inputData = MLUtils.loadLibSVMFile(sc, "健康状况训练集.txt")
    val splits = inputData.randomSplit(Array(0.7, 0.3), seed = 11L)
    val (trainingData, testData) = (splits(0), splits(1))
    val lr = new LogisticRegressionWithLBFGS()
    lr.setIntercept(true)
    val model = lr.run(trainingData)
    val result = testData.map{labeledpoint=>Math.abs(labeledpoint.label-model.predict(labeledpoint.features)) }
    println("正确率="+(1.0-result.mean()))
    
    /**
     *逻辑回归算法训练出来的模型，模型中的参数个数（w0....w6）=训练集中特征数(6)+1 
     */
    println(model.weights.toArray.mkString(" "))
    println(model.intercept)
    
    sc.stop()
  }
}



