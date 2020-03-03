package com.abigtomato.spark.scala.mllib.lr

import org.apache.log4j.{Level, Logger}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.{LabeledPoint, LinearRegressionWithSGD}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object LinearRegression {

  def main(args: Array[String]) {
    // 构建Spark对象
    val conf = new SparkConf().setAppName("LinearRegressionWithSGD").setMaster("local")
    val sc = new SparkContext(conf)
    Logger.getRootLogger.setLevel(Level.WARN)

    //读取样本数据
    val data = sc.textFile("lpsa.data")
    val examples :RDD[LabeledPoint]= data.map { line =>
      val parts = line.split(',')
      val y = parts(0)
      val xs = parts(1)
      LabeledPoint(parts(0).toDouble, Vectors.dense(parts(1).split(' ').map(_.toDouble)))
    }

    val train2TestData: Array[RDD[LabeledPoint]] = examples.randomSplit(Array(0.8, 0.2), 1L)

    /*
     *  迭代次数
     *  训练一个多元线性回归模型收敛（停止迭代）条件：
     *  	1、error值小于用户指定的error值
     *  	2、达到一定的迭代次数
     */
    val numIterations = 100

    //在每次迭代的过程中 梯度下降算法的下降步长大小    0.1 0.2 0.3 0.4
    val stepSize = 0.009

    val miniBatchFraction = 1

    val lrs = new LinearRegressionWithSGD()
    //让训练出来的模型有w0参数，就是有截距
    lrs.setIntercept(true)
    //设置步长
    lrs.optimizer.setStepSize(stepSize)
    //设置迭代次数
    lrs.optimizer.setNumIterations(numIterations)
    //每一次下山后，是否计算所有样本的误差值,1代表所有样本,默认就是1.0
    lrs.optimizer.setMiniBatchFraction(miniBatchFraction)

    val model = lrs.run(train2TestData(0))
    println("weights = "+  model.weights)
    println("intercept = "+ model.intercept)

    // 对样本进行测试
    val prediction = model.predict(train2TestData(1).map(_.features))
    val predictionAndLabel: RDD[(Double, Double)] = prediction.zip(train2TestData(1).map(_.label))

    val print_predict = predictionAndLabel.take(20)
    println("prediction" + "\t" + "label")
    for (i <- print_predict.indices) {
      println(print_predict(i)._1 + "\t" + print_predict(i)._2)
    }
    
    // 计算测试集平均误差
    val loss = predictionAndLabel.map {
      case (p, v) =>
        val err = p - v
        Math.abs(err)
    }.reduce(_ + _)
    val error = loss / train2TestData(1).count
    println("Test RMSE = " + error)
    // 模型保存
//    val ModelPath = "model"
//    model.save(sc, ModelPath)
//    val sameModel = LinearRegressionModel.load(sc, ModelPath)
    sc.stop()
  }

}
