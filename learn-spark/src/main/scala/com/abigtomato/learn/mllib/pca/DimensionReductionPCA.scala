package com.abigtomato.spark.scala.mllib.pca

import org.apache.spark.mllib.classification.LogisticRegressionWithLBFGS
import org.apache.spark.mllib.feature.{ PCA, StandardScaler }
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.{ SparkConf, SparkContext }
import org.apache.spark.mllib.regression.LabeledPoint

object DimensionReductionPCA {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("DimensionReductionPCA").setMaster("local[3]")
    val sc = new SparkContext(conf)

    val data = MLUtils.loadLibSVMFile(sc, "人体指标.txt").cache()
    val features = data.map(p => Vectors.dense(p.features.toArray))
    val scalerModel = new StandardScaler(withMean = true, withStd = true).fit(features)
    val scaledData = data.map(p => p.copy(features = scalerModel.transform(p.features.toDense))).cache()

    val standardData = scaledData.map(_.features)
    val pca = new PCA(3).fit(standardData)
    val pcaData = data.map(p => p.copy(features = pca.transform(p.features))).cache()

    val lr = new LogisticRegressionWithLBFGS()
    lr.setIntercept(true)
    val lr_pca = new LogisticRegressionWithLBFGS()
    lr_pca.setIntercept(true)
    val model = lr.run(scaledData)
    val model_pca = lr_pca.run(pcaData)
    val rightRate = scaledData.map { point =>
      val label = model.predict(point.features)
      val i = label == point.label match { case true => 1.0; case false => 0.0 }
      i
    }.mean()
    val rightRatePCA = pcaData.map { point =>
      val label = model_pca.predict(point.features)
      val i = label == point.label match { case true => 1.0; case false => 0.0 }
      i
    }.mean()
    println("原始特征正确率 = " + rightRate)
    println("PCA特征正确率 = " + rightRatePCA)

  }
}
