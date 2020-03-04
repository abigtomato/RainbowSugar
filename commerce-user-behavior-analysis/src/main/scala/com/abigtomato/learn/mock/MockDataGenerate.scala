package com.abigtomato.learn.mock

import java.util.{Random, UUID}

import com.abigtomato.learn.model.{ProductInfo, UserInfo, UserVisitAction}
import com.abigtomato.learn.utils.{DateUtils, StringUtils}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.mutable.ArrayBuffer

object MockDataGenerate {

  val USER_VISIT_ACTION_TABLE = "user_visit_action"
  val USER_INFO_TABLE = "user_info"
  val PRODUCT_INFO_TABLE = "product_info"

  /**
   * 入口函数
   * @param args
   */
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("MockData").setMaster("local[*]")

    val spark = SparkSession.builder().config(conf).enableHiveSupport().getOrCreate()

    val userVisitActionData = this.mockUserVisitActionData()
    val userInfoData = this.mockUserInfo()
    val productInfoData = this.mockProductInfo()

    val userVisitActionRDD = spark.sparkContext.makeRDD(userVisitActionData)
    val userInfoRDD = spark.sparkContext.makeRDD(userInfoData)
    val productInfoRDD = spark.sparkContext.makeRDD(productInfoData)

    import spark.implicits._

    val userVisitActionDF = userVisitActionRDD.toDF()
    insertHive(spark, USER_VISIT_ACTION_TABLE, userVisitActionDF)

    val userInfoDF = userInfoRDD.toDF()
    insertHive(spark, USER_INFO_TABLE, userInfoDF)

    val productInfoDF = productInfoRDD.toDF()
    this.insertHive(spark, PRODUCT_INFO_TABLE, productInfoDF)

    spark.close()
  }

  /**
   * 模拟用户行为数据
   * @return
   */
  private def mockUserVisitActionData(): Array[UserVisitAction] = {
    val searchKeywords = Array("华为手机", "联想笔记本", "小龙虾", "卫生纸", "吸尘器", "Lamer", "机器学习", "苹果", "洗面奶", "保温杯")
    val date = DateUtils.getToday()
    val actions = Array("search", "click", "order", "pay")  // 用户的4个行为：搜索，点击，下单，支付
    val random = new Random()
    val rows = ArrayBuffer[UserVisitAction]()

    // 一共生成100个用户（有重复）
    for (i <- 0 to 100) {
      val userId = random.nextInt(100)

      // 每个用户产生10个session会话
      for (j <- 0 to 10) {
        // 不可变的，全局唯一的，128bit的标识符UUID，用于表示一个sessionId
        val sessionId = UUID.randomUUID().toString.replace("-", "")
        // 在yyyy-MM-dd后面添加一个随机的小时时间（0-23）
        val baseActionTime = date + " " + random.nextInt(23)

        // 每个（userId + sessionId）随机生成0-100条用户访问数据
        for (k <- 0 to random.nextInt(100)) {
          // 随机生成0-10的页面id
          val pageId = random.nextInt(10)
          // 在yyyy-MM-dd HH后面添加随机的分和秒
          val actionTime = baseActionTime + ":" + StringUtils.fulfuill(String.valueOf(random.nextInt(59))) +
            ":" + StringUtils.fulfuill(String.valueOf(random.nextInt(59)))

          // 各字段初始化默认值
          var searchKeyword: String = null
          var clickCategoryId: Long = -1L
          var clickProductId: Long = -1L
          var orderCategoryIds: String = null
          var orderProductIds: String = null
          var payCategoryIds: String = null
          var payProductIds: String = null
          val cityId = random.nextInt(10).toLong

          // 随机生成一个用户在当前session中的行为
          val action = actions(random.nextInt(4))

          // 根据随机产生的用户行为决定对应字段的值
          action match {
            case "search" => searchKeyword = searchKeywords(random.nextInt(10))
            case "click" => clickCategoryId = random.nextInt(100).toLong
              clickProductId = random.nextInt(100).toLong
            case "order" => orderCategoryIds = random.nextInt(100).toString
              orderProductIds = random.nextInt(100).toString
            case "pay" => payCategoryIds = random.nextInt(100).toString
              payProductIds = random.nextInt(100).toString
          }

          rows += UserVisitAction(date, userId, sessionId, pageId,
            actionTime, searchKeyword, clickCategoryId,
            clickProductId, orderCategoryIds, orderProductIds,
            payCategoryIds, payProductIds, cityId)
        }
      }
    }
    rows.toArray
  }

  /**
   * 模拟用户数据
   * @return
   */
  private def mockUserInfo(): Array[UserInfo] = {
    val rows = ArrayBuffer[UserInfo]()
    val sexes = Array("male", "female")
    val random = new Random()

    for (i <- 0 to 100) {
      val userId = i
      val username = "user" + i
      val name = "name" + i
      val age = random.nextInt(60)
      val professional = "professional" + random.nextInt(100)
      val city = "city" + random.nextInt(100)
      val sex = sexes(random.nextInt(2))
      rows += UserInfo(userId, username, name, age, professional, city, sex)
    }
    rows.toArray
  }

  /**
   * 模拟商品数据
   * @return
   */
  private def mockProductInfo(): Array[ProductInfo] = {
    val rows = ArrayBuffer[ProductInfo]()
    val random = new Random()
    val productStatus = Array(0, 1)

    for (i <- 0 to 100) {
      val productId = i
      val productName = "product" + i
      val extendInfo = "{\"product_status\":" + productStatus(random.nextInt(2)) + "}"
      rows += ProductInfo(productId, productName, extendInfo)
    }
    rows.toArray
  }

  /**
   * 写入hive表
   * @param spark
   * @param tableName
   * @param dataDF
   */
  private def insertHive(spark: SparkSession, tableName: String, dataDF: DataFrame): Unit = {
    spark.sql("DROP TABLE IF EXISTS " + tableName)
    dataDF.write.saveAsTable(tableName)
  }
}
