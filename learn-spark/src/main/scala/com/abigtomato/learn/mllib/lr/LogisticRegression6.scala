package com.abigtomato.spark.scala.mllib.lr

import org.apache.spark.mllib.classification.LogisticRegressionWithLBFGS
import org.apache.spark.mllib.feature.StandardScaler
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.{SparkConf, SparkContext}
//import org.apache.spark.sql.SQLContext
/**
 * 方差归一化
 */
object LogisticRegression6 {

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("spark").setMaster("local[3]")
    val sc = new SparkContext(conf)
    /**
     * scalerModel 这个对象中已经有每一列的均值和方差
     * withStd:代表的是方差归一化
     * withMean:代表的是均值归一化
     * scalerModel：存放每一列的方差值
     * 
     * withMean默认为false, withStd默认为true
     * 当withMean=true，withStd=false时，向量中的各元素均减去它相应的均值。
     * 当withMean=true，withStd=true时，各元素在减去相应的均值之后，还要除以它们相应的标准差。 
     * 
     */
    val inputData = MLUtils.loadLibSVMFile(sc, "环境分类数据.txt")
    
    val vectors = inputData.map(_.features)
    val scalerModel = new StandardScaler(withMean=true, withStd=true).fit(vectors)
    
    val normalizeInputData = inputData.map{point =>  
      val label = point.label
      //对每一条数据进行了归一化
      val features = scalerModel.transform(point.features.toDense)
      println(features)
      new LabeledPoint(label,features)
    }

    
    val splits = normalizeInputData.randomSplit(Array(0.7, 0.3),100)
    val (trainingData, testData) = (splits(0), splits(1))
    val lr=new LogisticRegressionWithLBFGS()
    lr.setNumClasses(2)
//    val lr = new LogisticRegressionWithSGD()
    lr.setIntercept(true)
    val model = lr.run(trainingData)
    val result=testData.map{point=>Math.abs(point.label-model.predict(point.features)) }
    println("正确率="+(1.0-result.mean()))
    println(model.weights.toArray.mkString(" "))
    println(model.intercept)
  }
}
