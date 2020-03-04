package com.abigtomato.spark.scala.mllib.svm

import org.apache.spark.mllib.classification.SVMWithSGD
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.mllib.feature.StandardScaler
import org.apache.spark.mllib.regression.LabeledPoint

object SVM {

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("spark").setMaster("local[3]")
    val sc = new SparkContext(conf)
    val inputData = MLUtils.loadLibSVMFile(sc, "健康状况训练集.txt")
    val vectors = inputData.map(_.features)
    val scalerModel = new StandardScaler(withMean=true, withStd=true).fit(vectors)
    val normalizeInputData = inputData.map{point =>
      val label = point.label
      val features = scalerModel.transform(point.features.toDense)
      new LabeledPoint(label,features)
    }
    val splits = normalizeInputData.randomSplit(Array(0.7, 0.3))
    val (trainingData, testData) = (splits(0), splits(1))

    val svm = new SVMWithSGD()
    svm.setIntercept(true)
    val model = svm.run(trainingData)

    val rightRate = testData.map{p=>
      val label = p.label
      val predict = model.predict(p.features)
      val result = label==predict match {case true => 1 ; case false => 0}
      result
    }.mean()
    println("正确率：\t"+rightRate)
    println("w：\t"+model.weights)
  }
}
