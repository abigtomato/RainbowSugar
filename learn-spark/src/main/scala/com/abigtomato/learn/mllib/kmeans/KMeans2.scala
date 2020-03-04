package com.abigtomato.spark.scala.mllib.kmeans

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.mllib.clustering.KMeansModel
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.linalg.Vector

/**
 * 给kmeans指定中心点的位置
 */
object KMeans2 {

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("KMeans2").setMaster("local")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    val rdd = sc.parallelize(List(
      Vectors.dense(Array(-0.1, 0.0, 0.0)),
      Vectors.dense(Array(9.0, 9.0, 9.0)),
    Vectors.dense(Array(3.0, 2.0, 1.0))))
      
    //指定文件 kmeans_data.txt 中的六个点为中心点坐标。
    val centroids: Array[Vector] = sc.textFile("kmeans_data.txt")
        .map(_.split(" ").map(_.toDouble))
        .map(Vectors.dense(_))
        .collect()
 

    val model = new KMeansModel(clusterCenters=centroids)
    println("聚类个数 = "+model.k)
    //模型中心点
    model.clusterCenters.foreach { println }
    //预测指定的三条数据
    val result = model.predict(rdd)
    result.collect().foreach(println(_))
  }
}
