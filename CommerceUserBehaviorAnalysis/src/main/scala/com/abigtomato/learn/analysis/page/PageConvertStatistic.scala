package com.abigtomato.learn.analysis.page

import java.util.UUID

import com.abigtomato.learn.conf.ConfigurationManager
import com.abigtomato.learn.constant.Constants
import com.abigtomato.learn.model.{PageSplitConvertRate, UserVisitAction}
import com.abigtomato.learn.utils.{DateUtils, ParamUtils}
import net.sf.json.JSONObject
import org.apache.spark.SparkConf
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SaveMode, SparkSession}

import scala.collection.mutable

/**
 * 页面单跳转化率
 */
object PageConvertStatistic {

  def main(args: Array[String]): Unit = {
    val jsonStr = ConfigurationManager.config.getString(Constants.TASK_PARAMS)
    val taskParam = JSONObject.fromObject(jsonStr)

    val taskUUID = UUID.randomUUID().toString

    val conf = new SparkConf().setMaster("local").setAppName("PageConvertStatistic")
    val spark = SparkSession.builder().config(conf).enableHiveSupport().getOrCreate()

    // 获取用户行为数据
    val sessionId2ActionRDD: RDD[(String, UserVisitAction)] = this.getUserVisitAction(spark, taskParam)

    // 解析并转换taskParam参数中的页面跳转pageId
    val pageFlowStr = ParamUtils.getParam(taskParam, Constants.PARAM_TARGET_PAGE_FLOW)
    val pageFlowArray = pageFlowStr.split(",")  // [1, 2, 3, 4, 5, 6, 7]
    /*
     * 1.slice: [1, 2, 3, 4, 5, 6]
     * 2.tail: [2, 3, 4, 5, 6, 7]
     * 3.zip: [(1, 2), (2, 3), (3, 4), (4, 5), (5, 6), (6, 7)]
     * 4.map: ["1_2", "2_3", "3_4", "4_5", 5_6", "6_7"]
     */
    val targetPageSplit: Array[String] = pageFlowArray.slice(0, pageFlowArray.length - 1).zip(pageFlowArray.tail).map {
      case (page1, page2) => page1 + "_" + page2
    }
    val targetPageSplitBd: Broadcast[Array[String]] = spark.sparkContext.broadcast(targetPageSplit)

    // 获取所有符和taskParam参数的pageId的页面跳转访问次数
    val pageSplitCountMap: collection.Map[String, Long] = sessionId2ActionRDD.groupByKey().flatMap {
      case (_, iterableAction) =>
        val pageList = iterableAction.toList.sortWith((item1, item2) => {
          DateUtils.parseTime(item1.action_time).getTime < DateUtils.parseTime(item2.action_time).getTime
        }).map(action => action.page_id)

        pageList.slice(0, pageList.length - 1).zip(pageList.tail).map {
          case (page1, page2) => page1 + "_" + page2
        }.filter(targetPageSplitBd.value.contains(_)).map((_, 1))
    }.countByKey()

    // 获取taskParam参数第一个页面的访问次数
    val startPageId = pageFlowArray(0).toLong
    val startPageCount = sessionId2ActionRDD.filter {
      case (_, action) => action.page_id == startPageId
    }.count()

    // 获取页面转化率
    val pageSplitRatio: mutable.Map[String, Double] = this.getPageConvert(spark, taskUUID, targetPageSplit, startPageCount, pageSplitCountMap)
    val convertStr = pageSplitRatio.map { case (pageSplit, ratio) => pageSplit + "=" + ratio }.mkString("|")

    // 写入结果到数据库
    import spark.implicits._
    val convertRate = PageSplitConvertRate(taskUUID, convertStr)
    spark.sparkContext.makeRDD(Array(convertRate)).toDF().write
      .format("jdbc")
      .option("url", ConfigurationManager.config.getString(Constants.JDBC_URL))
      .option("user", ConfigurationManager.config.getString(Constants.JDBC_USER))
      .option("password", ConfigurationManager.config.getString(Constants.JDBC_PASSWORD))
      .option("dbtable", "page_split_convert_rate")
      .mode(SaveMode.Append)
      .save()

    spark.close()
  }

  /**
   * 获取页面转化率
   * @param spark
   * @param taskUUID
   * @param targetPageSplit
   * @param startPageCount
   * @param pageSplitCountMap
   * @return
   */
  def getPageConvert(spark: SparkSession, taskUUID: String, targetPageSplit: Array[String], startPageCount: Long,
                     pageSplitCountMap: collection.Map[String, Long]): mutable.Map[String, Double] = {
    val pageSplitRatio = new mutable.HashMap[String, Double]()
    var lastPageCount = startPageCount.toDouble

    for (pageSplit <- targetPageSplit) {
      val currentPageSplitCount = pageSplitCountMap(pageSplit).toDouble
      val ratio = currentPageSplitCount / lastPageCount
      pageSplitRatio.put(pageSplit, ratio)
      lastPageCount = currentPageSplitCount
    }
    pageSplitRatio
  }

  /**
   * 获取用户行为数据
   * @param spark
   * @param taskParam
   * @return
   */
  def getUserVisitAction(spark: SparkSession, taskParam: JSONObject): RDD[(String, UserVisitAction)] = {
    val startDate = ParamUtils.getParam(taskParam, Constants.PARAM_START_DATE)
    val endDate = ParamUtils.getParam(taskParam, Constants.PARAM_END_DATE)

    import spark.implicits._
    spark.sql(s"select * from user_visit_action where date >= '$startDate' and date <= '$endDate'")
      .as[UserVisitAction].rdd.map(item => (item.session_id, item))
  }
}
