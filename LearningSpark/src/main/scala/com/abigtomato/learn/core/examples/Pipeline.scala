package com.abigtomato.spark.scala.core.examples

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object Pipeline {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)

    val session = SparkSession
      .builder()
      .appName("Pipeline")
      .master("local")
      .getOrCreate()
    val sc = session.sparkContext

    sc.parallelize(List("Golang", "Python", "Scala"))
      .map { x =>
        println("map")
        x + "~"
      }.filter { x =>
        println("filter")
        !x.contains("Java")
      }.count()

    session.close()
  }
}
