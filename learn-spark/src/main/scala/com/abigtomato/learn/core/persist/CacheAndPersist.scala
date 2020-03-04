package com.abigtomato.spark.scala.core.persist

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel

/**
  * cache()和persist()注意问题：
  *   - 1.cache()和persist()持久化单位是partition，cache()和persist()是懒执行算子，需要action类算子触发执行；
  *   - 2.对一个RDD使用cache或者persist之后可以赋值给一个变量，下次直接使用这个变量就是使用的持久化的数据。也可以直接对RDD进行cache或者persist不赋值给变量；
  *   - 3.如果采用第二种方式赋值给变量的话，后面不能紧跟action算子；
  *   - 4.cache()和persist()的数据在当前application执行完成之后会自动清除。
  */
object CacheAndPersist {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)

    val session = SparkSession
      .builder()
      .appName("cache_and_persist")
      .master("local")
      .getOrCreate()
    val sc = session.sparkContext

    val path = "D:\\WorkSpace\\learn.scala\\LearnSpark\\src\\main\\resources\\tree_data.txt"

    var textRDD = sc.textFile(s"file:///${path}")

    // 使用cache()或persist()算子将RDD的分区缓存到内存中
//    textRDD.cache()
    textRDD = textRDD.persist(StorageLevel.MEMORY_ONLY) // StorageLevel.MEMORY_ONLY级别的persist() = cache()

    val startTime = System.currentTimeMillis()
    // 控制类算子是必须经过action类算子触发才会执行，所以此时的textRDD是从磁盘中读取的
    val count = textRDD.count()
    val endTime = System.currentTimeMillis()
    println(s"count: ${count}\tdurations: ${endTime - startTime}ms")

    val startTime2 = System.currentTimeMillis()
    // 因为之前的action类操作让textRDD缓存到内存中，所以此次textRDD是从内存中读取的
    val count2 = textRDD.count()
    val endTime2 = System.currentTimeMillis()
    println(s"count: ${count2}\tdurations: ${endTime2 - startTime2}ms")

    // 手动释放内存
    textRDD.unpersist()

    session.close()
  }
}
