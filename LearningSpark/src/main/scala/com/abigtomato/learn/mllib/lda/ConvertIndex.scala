package com.abigtomato.spark.scala.mllib.lda

import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.{SparkContext, SparkConf}

import scala.collection.mutable.ArrayBuffer

/**
  *
  * 把原始的新闻向量转换成LDA所能接受的格式
  */
object ConvertIndex {

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("lda").setMaster("local[3]")
    val sc = new SparkContext(conf)
    val data = sc.textFile("分词结果.txt").cache()
    val wordlist = data.flatMap{line =>
      val ss = line.split("\t")
      val list = new ArrayBuffer[String]()
      if(ss.length==2){
        val wordsNum = ss(1).split(" ")
        for(w<-wordsNum){
          val word = w.split(":")(0)
          list.append(word)
        }
      }
      list.toSet
    }.distinct().collect()
    val wordIndex = wordlist.zipWithIndex
    sc.parallelize(wordIndex).saveAsTextFile("词索引")

    val map = wordIndex.toMap
    val size = wordIndex.length + 1
    val indexlist = data.map{line =>
      val sb = new StringBuffer()
      val ss = line.split("\t")
      val title = ss(0)
      val indexlist = new ArrayBuffer[Int]()
      val numlist = new ArrayBuffer[Double]()
      if (ss.length==2){
        val wordNum = ss(1).split(" ")
        for(wn <- wordNum){
          val ww = wn.split(":")
          indexlist.append(map.getOrElse(ww(0),0))
          numlist.append(ww(1).toDouble)
        }
      }
      (title, Vectors.sparse(size, indexlist.toArray, numlist.toArray))
    }
    indexlist.saveAsObjectFile("新闻-向量")
  }
}
