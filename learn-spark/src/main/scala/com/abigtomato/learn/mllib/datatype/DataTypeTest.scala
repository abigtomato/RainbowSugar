package com.abigtomato.spark.scala.mllib.datatype

import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.mllib.util.MLUtils

object DataTypeTest {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("DataTypeTest")
    val sc = new SparkContext(conf)

    val dv: Vector = Vectors.dense(1.0, 0.0, 3.0)
    val sv1: Vector = Vectors.sparse(3, Array(0, 2), Array(1.0, 3.0))
    val sv2: Vector = Vectors.sparse(3, Seq((0, 1.0), (2, 3.0)))
    
    
    println(sv1.toDense)

    val pos = LabeledPoint(1.0, Vectors.dense(1.0, 0.0, 3.0))
    val neg = LabeledPoint(0.0, Vectors.sparse(3, Array(0, 2), Array(1.0, 3.0)))
    println(pos.features)
    println(pos.label)
    println(neg.features.toDense)

    val examples: RDD[LabeledPoint] = MLUtils.loadLibSVMFile(sc, "sample_libsvm_data.txt")

    examples.foreach { x =>
      {
        val label = x.label
        val features = x.features
        println("label:" + label + "\tfeatures:" + features.toDense)
      }
    }

    sc.stop()

  }
}