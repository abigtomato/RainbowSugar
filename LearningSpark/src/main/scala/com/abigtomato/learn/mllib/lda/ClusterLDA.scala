package com.abigtomato.spark.scala.mllib.lda

import org.apache.spark.mllib.clustering.{LDA, DistributedLDAModel}
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.mllib.linalg.Vector

object ClusterLDA {

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("lda").setMaster("local[3]")
    val sc = new SparkContext(conf)
    val data = sc.objectFile[(String,Vector)]("新闻-向量").zipWithIndex()
    val paperId=data.map(p=>(p._2,p._1._1))
    // Index documents with unique IDs
    val corpus = data.map(p=>(p._2,p._1._2)).cache()
    // 取语料百分之一到百分之五的比例作为主题
    val ldaModel = new LDA().setK(50).run(corpus)
    ldaModel.asInstanceOf[DistributedLDAModel]
      .topicDistributions.map(e=>e._1+"\t"+e._2).foreach(println(_))
    // Save and load model.
//         ldaModel.save(sc, "LDA模型")
//        val sameModel = DistributedLDAModel.load(sc, "LDA模型")//.asInstanceOf[LDAModel]
//        val topicDistributions=sameModel.topicDistributions
//        val paperVector=topicDistributions.join(paperId).map(p=>p._2._2+"\t"+p._2._1.toArray.mkString(" "))
//        paperVector.saveAsTextFile("新闻向量")
  }
}
