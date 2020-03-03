package com.abigtomato.spark.scala.mllib.stock

import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.classification.{LogisticRegressionWithLBFGS, LogisticRegressionWithSGD, LogisticRegressionModel}
import org.apache.spark.mllib.feature.{PCA, PCAModel, StandardScaler, StandardScalerModel}
import org.apache.spark.mllib.optimization.SquaredL2Updater
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.tree.RandomForest
import org.apache.spark.mllib.tree.model.RandomForestModel
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.rdd.RDD

class Stock(scalerModel:StandardScalerModel, logisticRegressionModel: LogisticRegressionModel, randomForestModel: RandomForestModel, pCAModel: PCAModel) extends scala.Serializable{

  def save(sc:SparkContext, path:String): Unit ={
    this.logisticRegressionModel.save(sc,path+"逻辑回归")
    this.randomForestModel.save(sc,path+"随机森林")
    sc.parallelize(Seq(this.scalerModel)).saveAsObjectFile(path+"归一化因子")
    sc.parallelize(Seq(this.pCAModel)).saveAsObjectFile(path+"pca模型")

  }

  def predict(vector: Vector, threshold:Double): Double ={
    // 也要对进来的向量进行归一化
    val lp = this.scalerModel.transform(vector.toDense)
    // 是不是还有PCA一下
    val feature = this.pCAModel.transform(lp)
    // 现在两个模型都是输出0-1之间的数,怎么能叫两个模型一起工作?要预测的准
    // 两个模型给出的结果一致,才最终给出结果
    // 要想还能更准,可以设定阈值Threshold>0.8,大于0.8预测涨,小于1-0.8的时候预测跌,在0.2和0.8之间不做预测
    // 现在实际上变成了3类分的问题了
    val result1 = this.logisticRegressionModel.predict(feature) match {
      case x if x > threshold => 1
      case x if x < (1-threshold) => -1
      case _ => 0
    }
    val result2 = this.randomForestModel.predict(feature) match {
      case x if x > threshold => 1
      case x if x < (1-threshold) => -1
      case _ => 0
    }
    
    if(result1==result2){
      result1
    }else{
      0
    }
  }

  def calRightAndRecall(inputData : RDD[LabeledPoint], threshold: Double): (Array[(Double,Double)],Array[(Double,Double)]) ={
    val newLabeledData = inputData.map(point => point.copy(label=point.label*2-1)).cache()
    val result = newLabeledData.map{point=>
      val predicted = this.predict(point.features,threshold)
      val result = predicted == point.label match {
        case true => 1
        case false => 0
      }
      //预测类别，是否正确
      (predicted, result)
//      (predicted, (result, 1))
    }.filter(_._1 != 0)
      .aggregateByKey((0,0))((temp,value)=>(temp._1+value,temp._2+1),(v1,v2)=>(v1._1+v2._1,v1._2+v2._2))
    //        .reduceByKey((v1,v2)=>(v1._1+v2._1,v1._2+v2._2))
    // 正确率用预测对的除以总共预测的数量
    // 召回率计算的时候正确的个数和总个数需要参与,所以也往后传递
    val rightRate = result.mapValues(p=>(p._1.toDouble/p._2, p._1, p._2)).collect()
    // 每个元素的内容就是 (类别,(正确率,预测正确的数量,总数))
//    rightRate.foreach{case(label,rate)=>println("类别\t"+label+"\t正确率为：\t"+rate)}
    val recallRate = new Array[(Double,Double)](rightRate.length)
    for(i<-0 until rightRate.size){
      val count = newLabeledData.filter(_.label==rightRate(i)._1).count()
      // 召回率用预测对的数除以对应的类别总共有多少个
      val recall = rightRate(i)._2._1.toDouble/count
      recallRate(i)=(rightRate(i)._1, recall)
    }
    (rightRate.map(p=>(p._1,p._2._1)),recallRate)
  }
}


object Stock {

  def load(sc:SparkContext, path:String): Stock ={
    val logisticRegressionModel = LogisticRegressionModel.load(sc,path+"逻辑回归")
    val randomForestModel = RandomForestModel.load(sc,path+"随机森林")
    val scalerModel = sc.objectFile[StandardScalerModel](path+"归一化因子").collect()(0)
    val pcaModel = sc.objectFile[PCAModel](path+"pca模型").collect()(0)
    new Stock(scalerModel, logisticRegressionModel, randomForestModel, pcaModel)
  }

  def getStandardScaler(inputData : RDD[LabeledPoint]): StandardScalerModel = {
    val vectors = inputData.map(_.features)
    // 均值归一化 并且 方差归一化
    val scalerModel = new StandardScaler(withMean = true, withStd = true).fit(vectors)
    scalerModel
  }

  def copyData(input : RDD[LabeledPoint], num:Int): RDD[LabeledPoint] ={
    input.flatMap(List.fill(num)(_))
  }

  // 随机抽取补全
  // 倍数增加平衡数据
  def balanceData(normalizedInputData : RDD[LabeledPoint]): RDD[LabeledPoint] ={

    val category1Data = normalizedInputData.filter(_.label == 1).cache()
    val category0Data = normalizedInputData.filter(_.label == 0).cache()
    val count1 = category1Data.count().toDouble
    val count0 = category0Data.count().toDouble
    val (data1, data0) = count1 > count0 match {
      case true =>
        val rate = (5*(count1 / count0)).toInt
        (copyData(category1Data, 5) , copyData(category0Data, rate))
      case false =>
        val rate = (5*(count0 / count1)).toInt
        (copyData(category1Data, rate) , copyData(category0Data, 5))
    }
    // 释放下
    category1Data.unpersist()
    category0Data.unpersist()
    data1.union(data0)
  }

  def compressFeature(normalizedInputData : RDD[LabeledPoint]): (RDD[LabeledPoint], PCAModel) ={
    val standardData = normalizedInputData.map(_.features)
    val pca = new PCA(5).fit(standardData)
    val pcaData = normalizedInputData.map(point => point.copy(features = pca.transform(point.features)))
    (pcaData,pca)
  }

  def trainLogistic(input: RDD[LabeledPoint]): LogisticRegressionModel = {
    val lr = new LogisticRegressionWithLBFGS
    // 设置w0
    lr.setIntercept(true)
    // 设置L2范式
    lr.optimizer.setUpdater(new SquaredL2Updater)
    // 设置Lambda系数
    lr.optimizer.setRegParam(0.4)
    // 为了多拿点信息不要阈值,不输出类别号,直接输出0-1的值
    val model = lr.run(input).clearThreshold()
    model
//    model.asInstanceOf[LogisticRegressionModel]
  }

  def trainRandomForest(input: RDD[LabeledPoint]): RandomForestModel ={
    // 设置离散变量
    val categoricalFeatureInfo = Map[Int,Int]()
    // 设置树的个数
    val numTrees = 3
    // 设置怎么样选择特征
    val featureSubsetStrategy = "auto"
    // 设置不纯度,这里用方差不纯度,用来测量连续数据的不纯度的,熵是用来测量离散数据的不纯度的
    val impurity = "variance"
    // 设置树的层次
    val maxDepth = 2
    // 设置离散特征离散化
    val maxBins =32
    val model = RandomForest.trainRegressor(input, categoricalFeatureInfo, numTrees,featureSubsetStrategy,impurity,maxDepth,maxBins)
    model
  }

  def run(inputData : RDD[LabeledPoint]): Stock = {

    // 模型归一化,让模型对数据进行归一化
    val scalerModel = getStandardScaler(inputData)
    val normalizedInputData = inputData.map{ point =>
      // 这里其实可以广播变量scalerModel,让每个节点仅持有一份
      point.copy(features = scalerModel.transform(point.features.toDense))
    }
    // 平衡数据
    val balancedData = balanceData(normalizedInputData)
    // 特征压缩
    val (newLabeledFeatures, pCAModel) = compressFeature(balancedData)

    // 下面将转换后的newLabeledFeatures数据交给算法去计算模型
    // 选择两种算法,逻辑回归和随机森林
    // 持久化的原因除了用了两次算法,还有训练的过程是个迭代的过程,可能会涉及反复的读取
    newLabeledFeatures.cache()
    val logisticRegressionModel = trainLogistic(newLabeledFeatures)
    val randomForestModel = trainRandomForest(newLabeledFeatures)

    new Stock(scalerModel, logisticRegressionModel, randomForestModel, pCAModel)
  }

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("stock").setMaster("local[3]")
    val sc = new SparkContext(conf)
    val input = MLUtils.loadLibSVMFile(sc, "002089特征.txt").cache()
    val model = Stock.run(input)
    // 保存我们的模型
    //   model.save(sc,"002089")
    //  val model2=Stock.load(sc,"002089")
    // 预测单条数据
//    model.predict(vector)
    val (rightRate, recallRate) = model.calRightAndRecall(input, 0.55)
    println(rightRate.map(p=>"类别\t"+p._1+"\t正确率\t"+p._2).mkString("\n"))
    println(recallRate.map(p=>"类别\t"+p._1+"\t召回率\t"+p._2).mkString("\n"))
  }
}
