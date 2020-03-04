package com.abigtomato.spark.scala.mllib

import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.{LabeledPoint, LinearRegressionModel, LinearRegressionWithSGD}
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.{SparkConf, SparkContext}

object LinearRegression {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("LinearRegression").setMaster("local[3]")
    val sc = new SparkContext(conf)

    val path = "D:\\WorkSpace\\learn.scala\\LearnSpark\\src\\main\\resources\\data.txt"

    // 加载特定格式的文件为训练样本（RDD[LabeledPoint]）
    val data = MLUtils.loadLibSVMFile(sc, path).cache()

    // 手动实现加载文件并映射为训练集
    val data1 = sc.textFile(path).map(line => {
      val parts = line.split(" ")
      // 参数一为样本标签，参数二为样本特征
      LabeledPoint(parts(0).toDouble, Vectors.dense(parts.tail.map(elem => {
        elem.split(":")(1).toDouble
      })))
    }).cache()

    // 迭代次数
    val numIterations = 100
    // 下降步长（学习率）
    val stepSize = 0.00000001
    // 使用梯度下降的线下回归算法训练模型
    val model = LinearRegressionWithSGD.train(data, numIterations, stepSize)

    val valuesAndPreds = data.map(point => {
      // 使模型通过样本特征预测标签
      val prediction = model.predict(point.features)
      println(s"[真实值]: ${point.label}; [预测值]: ${prediction}")
      (point.label, prediction)
    })

    val MSE = valuesAndPreds.map{ case(v, p) => math.pow((v - p), 2) }.mean()
    println("训练后模型的均方误差为: " + MSE)

    // 保存模型
    model.save(sc, "target/tmp/scalaLinearRegressionWithSGDModel")
    // 加载模型
    val sameModel = LinearRegressionModel.load(sc, "target/tmp/scalaLinearRegressionWithSGDModel")

    sc.stop()
  }
}
