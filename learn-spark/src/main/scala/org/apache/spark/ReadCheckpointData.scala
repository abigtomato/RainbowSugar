package org.apache.spark

/**
 * 在scala中 sparkContext.checkpointFile(path) 只能在org.apache.spark这个包下的类才能调用
 */
object ReadCheckpointData {
  def main(args: Array[String]): Unit = {
    val conf =new SparkConf()
      .setAppName("ReadCheckpointData")
      .setMaster("local")
    val sc = new SparkContext(conf)

    val result = sc.checkpointFile("./data/checkpoint/25675151-18ed-43bf-b4ce-dfc93417f4fd/rdd-1")
    result.foreach(println)

    sc.stop()
  }
}
