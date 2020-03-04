package com.abigtomato.spark.scala.mllib

import org.apache.log4j.{Level, Logger}
import org.apache.spark.mllib.recommendation.{ALS, MatrixFactorizationModel, Rating}
import org.apache.spark.{SparkConf, SparkContext}

object Recommendation {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)

    val conf = new SparkConf().setMaster("local[3]").setAppName("Recommendation")
    val sc = new SparkContext(conf)

    // 加载数据并构建评分矩阵
    val path = "D:\\WorkSpace\\learn.scala\\LearnSpark\\src\\main\\resources\\test.data"
    val data = sc.textFile(path)
    val ratings = data.map(_.split(",") match {
      case Array(user, item, rate) => Rating(user.toInt, item.toInt, rate.toDouble)
    })

    // 训练ALS模型
    val rank = 10
    val numIterations = 10
    val model = ALS.train(ratings, rank, numIterations, 0.01)

    // 构建用户-物品矩阵
    val usersProducts = ratings.map {
      case Rating(user, product, rate) => (user, product)
    }

    // 预测评分数据
    val prediction = model.predict(usersProducts).map {
      case Rating(user, product, rate) => ((user, product), rate)
    }

    // 连接真实值和预测值
    val ratesAndPreds = ratings.map {
      case Rating(user, product, rate) => ((user, product), rate)
    }.join(prediction)

    // 计算误差
    val MSE = ratesAndPreds.map {
      case ((user, product), (realRate, predRate)) => {
        println(s"[用户ID]: ${user}; [物品ID]: ${product}; [真实值]: ${realRate}; [预测值]: ${predRate}")
        val err = (realRate - predRate)
        err * err
      }
    }.mean()
    println("预测的均方误差为: " + MSE)

    // 模型的保存和加载
    model.save(sc, "target/tmp/myCollaborativeFilter")
    val sameModel = MatrixFactorizationModel.load(sc, "target/tmp/myCollaborativeFilter")

    sc.stop()
  }
}
