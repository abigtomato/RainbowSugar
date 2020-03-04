package com.abigtomato.spark.scala.mllib.kmeans

import org.apache.spark.mllib.clustering.{KMeans, KMeansModel}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}
/**
 * 通过数据集使用kmeans训练模型
 */
object KMeansScala {
  def main(args: Array[String]): Unit = {
    
    //1 构建Spark对象
    val conf = new SparkConf().setAppName("KMeans").setMaster("local")
    val sc = new SparkContext(conf)

    // 读取样本数据1，格式为LIBSVM format
    val data = sc.textFile("kmeans_data.txt")
    val parsedData = data.map(s => Vectors.dense(s.split(' ').map(_.toDouble))).cache()
    

    val numClusters = 2
    val numIterations = 100
    val model = new KMeans().
      //设置聚类的类数
      setK(numClusters).
      //设置找中心点最大的迭代次数
      setMaxIterations(numIterations).run(parsedData)
      
    //2个中心点的坐标
    val centers = model.clusterCenters
    val k = model.k
    println("中心点坐标：")
    centers.foreach(println)
    println("类别 k = "+k)
    //保存模型
    model.save(sc, "./Kmeans_model")
    //加载模型
    val sameModel = KMeansModel.load(sc, "./Kmeans_model")
    println("样本分类号：" + sameModel.predict(Vectors.dense(1,1,1)))


    //SparkSQL读取显示2个中心点坐标
    val spark = SparkSession.builder().getOrCreate()
    spark.read.parquet("./Kmeans_model/data").show()

  }
}