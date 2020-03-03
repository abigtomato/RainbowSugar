package com.abigtomato.spark.scala.mllib

import org.apache.log4j.{Level, Logger}
import org.apache.spark.mllib.tree.DecisionTree
import org.apache.spark.mllib.tree.model.DecisionTreeModel
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.{SparkConf, SparkContext}

object DecisionTreeApp {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)

    val conf = new SparkConf().setAppName("DecisionTreeApp").setMaster("local[3]")
    val sc = new SparkContext(conf)

    // 加载数据集
    val path = "D:\\WorkSpace\\learn.scala\\LearnSpark\\src\\main\\resources\\tree_data.txt"
    val data = MLUtils.loadLibSVMFile(sc, path)

    // 随机拆分训练集和测试集
    val splits = data.randomSplit(Array(0.7, 0.3))
    val (trainingData, testData) = (splits(0), splits(1))

    // 决策树模型训练
    val numClasses = 2
    val categoricalFeaturesInfo = Map[Int, Int]()
    val impurity = "gini"
    val maxDepth = 5
    val maxBins = 32
    val model = DecisionTree.trainClassifier(trainingData, numClasses,
      categoricalFeaturesInfo, impurity, maxDepth, maxBins)

    // 模型训练
    val labelAndPreds = testData.map { point =>
      val prediction = model.predict(point.features)
      (point.label, prediction)
    }

    // 计算误差
    val testErr = labelAndPreds.filter {
      tuple => tuple._1 != tuple._2
    }.count().toDouble / testData.count()
    println("分类误差: " + testErr)
    println("训练的决策树模型: \n" + model.toDebugString)

    // 模型的保存与加载
    model.save(sc, "target/tmp/myDecisionTreeClassificationModel")
    val sameModel = DecisionTreeModel.load(sc, "target/tmp/myDecisionTreeClassificationModel")

    sc.stop()
  }
}
