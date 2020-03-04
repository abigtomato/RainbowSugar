package com.abigtomato.spark.scala.mllib.lr

import org.apache.spark.ml.feature.MinMaxScaler
import org.apache.spark.ml.linalg
import org.apache.spark.mllib
import org.apache.spark.mllib.classification.LogisticRegressionWithLBFGS
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.sql.SparkSession

/**
 * 最大最小值归一化
 */
object LogisticRegression7 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("spark").master("local").getOrCreate()
    spark.sparkContext.setLogLevel("Error")
    /**
     * 加载生成的DataFrame自动有两列：label features
     */
    val df = spark.read.format("libsvm").load("环境分类数据.txt")
    /**
     * MinMaxScaler fit需要DataFrame类型数据
     * setInputCol：设置输入的特征名
     * setOutputCol：设置归一化后输出的特征名
     * 
     */
    df.show(false)

    val minMaxScalerModel = new MinMaxScaler()
                            .setInputCol("features")
                            .setOutputCol("scaledFeatures")
                            .fit(df)
    /**
     * 将所有数据归一化
     */
    val features = minMaxScalerModel.transform(df)
    features.show(false)

    val normalizeInputData = features.rdd.map(row=>{
      val label = row.getAs("label").toString().toDouble
      val dense: linalg.DenseVector = (row.getAs[linalg.DenseVector]("scaledFeatures")).toDense
      val vector: mllib.linalg.Vector = org.apache.spark.mllib.linalg.Vectors.fromML(dense)
      //      val dense = (row.getAs("scaledFeatures")).asInstanceOf[DenseVector]
      new LabeledPoint(label,vector)
    })

    val splits = normalizeInputData.randomSplit(Array(0.7, 0.3),11L)
    val (trainingData, testData) = (splits(0), splits(1))
    val lr=new LogisticRegressionWithLBFGS()
    lr.setIntercept(true)
    val model = lr.run(trainingData)
    val result=testData.map{point=>Math.abs(point.label-model.predict(point.features)) }
    println("正确率="+(1.0-result.mean()))
    println(model.weights.toArray.mkString(" "))
    println(model.intercept)

  }
}