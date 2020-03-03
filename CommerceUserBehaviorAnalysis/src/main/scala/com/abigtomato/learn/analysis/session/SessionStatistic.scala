package com.abigtomato.learn.analysis.session

import java.util.{Date, UUID}

import com.abigtomato.learn.conf.ConfigurationManager
import com.abigtomato.learn.constant.Constants
import com.abigtomato.learn.model.{AggrInfo, CategoryIdCount, FilterParam, FullInfo, SessionAggrStat, SessionRandomExtract, Top10Category, Top10Session, UserInfo, UserVisitAction}
import com.abigtomato.learn.utils.{DateUtils, NumberUtils, ParamUtils, StringUtils}
import net.sf.json.JSONObject
import org.apache.spark.SparkConf
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SaveMode, SparkSession}

import scala.collection.mutable
import scala.collection.mutable.{ArrayBuffer, ListBuffer}
import scala.util.Random

/**
 * session相关统计需求
 */
object SessionStatistic {

  def main(args: Array[String]): Unit = {
    // 从配置文件中获取筛选条件
    val jsonStr = ConfigurationManager.config.getString(Constants.TASK_PARAMS)
    val taskParam = JSONObject.fromObject(jsonStr)

    // 创建task统计结果的主键
    val taskUUID = UUID.randomUUID().toString

    // 创建SparkSession
    val conf = new SparkConf().setAppName("session").setMaster("local[*]")
    val spark = SparkSession.builder().config(conf).enableHiveSupport().getOrCreate()

    // 获取动作表数据，UserVisitAction类型的RDD
    val actionRDD: RDD[UserVisitAction] = this.getOriActionRDD(spark, taskParam)

    /**
     * 需求1：session各维度时长数和步长数占比统计
     */
    val sessionId2ActionRDD: RDD[(String, UserVisitAction)] = actionRDD.map(item => (item.session_id, item))

    val sessionId2GroupActionRDD: RDD[(String, Iterable[UserVisitAction])] = sessionId2ActionRDD.groupByKey()

    sessionId2GroupActionRDD.cache()

    val sessionId2FullInfoRDD: RDD[(String, FullInfo)] = this.getSessionFullInfo(spark, sessionId2GroupActionRDD)

    val scAndRddTuple: (SessionAccumulator, RDD[(String, FullInfo)]) = this.getSessionFilteredRDD(spark, taskParam, sessionId2FullInfoRDD)

    val sessionId2FilterRDD: RDD[(String, FullInfo)] = scAndRddTuple._2

    sessionId2FilterRDD.count()

    this.saveSessionRatio(spark, taskUUID, scAndRddTuple._1.value)

    /**
     * 需求2：session随机抽取
     */
    this.sessionRandomExtract(spark, taskUUID, scAndRddTuple._2)

    /**
     * 需求3：统计点击，下单，支付三种动作的TOP10热门品类
     */
    val sessionId2FilterActionRDD: RDD[(String, UserVisitAction)] = sessionId2ActionRDD
      .join(sessionId2FilterRDD)
      .map { case (sessionId, (action, _)) => (sessionId, action)}

    val top10CategoryArray: Array[(SortKey, Long)] = this.top10PopularCategories(spark, taskUUID, sessionId2FilterActionRDD)

    /**
     * 需求4：top10热门品类下的top10活跃session统计
     *  - 对top10热门品类中的每个品类都取top10的活跃session
     *  - 评判活跃session的指标是一个session中对该品类的点击次数
     */
    this.top10ActiveSession(spark, taskUUID, sessionId2FilterActionRDD, top10CategoryArray)

    spark.close()
  }

  def top10ActiveSession(spark: SparkSession, taskUUID: String, sessionId2FilterActionRDD: RDD[(String, UserVisitAction)],
                         top10CategoryArray: Array[(SortKey, Long)]): Unit = {
    val cidArray = top10CategoryArray.map(_._2)
    val cidArrayBd = spark.sparkContext.broadcast(cidArray)

    // 所有符合过滤条件，并且点击过top10热门品类的action
    val sessionId2ActionRDD: RDD[(String, UserVisitAction)] = sessionId2FilterActionRDD.filter(item => {
      val cid = item._2.click_category_id
      cidArrayBd.value.contains(cid)
    })

    val cid2SessionCountRDD: RDD[(Long, (String, Long))] = sessionId2ActionRDD
      .groupByKey()
      .flatMap {
        case (sessionId, iterableAction) =>
          val categoryCountMap = new mutable.HashMap[Long, Long]()
          for (action <- iterableAction) {
            val cid = action.click_category_id
            categoryCountMap.get(cid) match {
              case None => categoryCountMap += (cid -> 0)
              case Some(_) => categoryCountMap.update(cid, categoryCountMap(cid) + 1)
            }
          }
          // 记录了当前category对应的所有session和点击次数
          for ((cid, count) <- categoryCountMap)
            yield (cid, (sessionId, count))
      }

    val top10SessionRDD: RDD[Top10Session] = cid2SessionCountRDD
      .groupByKey()
      .flatMap {
        case (cid, iterableSessionCount) =>
          // true：item1排在前面
          // false：item2排在前面
          iterableSessionCount.toList.sortWith((item1, item2) => {
            item1._2 > item2._2
          }).take(10).map(item => {
            Top10Session(taskUUID, cid, item._1, item._2)
          })
      }

    import spark.implicits._
    top10SessionRDD.toDF().write
      .format("jdbc")
      .option("url", ConfigurationManager.config.getString(Constants.JDBC_URL))
      .option("user", ConfigurationManager.config.getString(Constants.JDBC_USER))
      .option("password", ConfigurationManager.config.getString(Constants.JDBC_PASSWORD))
      .option("dbtable", "top10_session")
      .mode(SaveMode.Append)
      .save()
  }

  def getFullCount(cid2CidRDD: RDD[(Long, Long)], cid2ClickCountRDD: RDD[(Long, Long)],
                   cid2OrderCountRDD: RDD[(Long, Long)], cid2PayCountRDD: RDD[(Long, Long)]): RDD[(Long, CategoryIdCount)] = {
    cid2CidRDD.leftOuterJoin(cid2ClickCountRDD).map {
      case (cid, (_, option)) => (cid, CategoryIdCount(cid, option.getOrElse(0L), 0L, 0L))
    }.leftOuterJoin(cid2OrderCountRDD).map {
      case (cid, (categoryIdCount, option)) =>
        categoryIdCount.orderCount = option.getOrElse(0L)
        (cid, categoryIdCount)
    }.leftOuterJoin(cid2PayCountRDD).map {
      case (cid, (categoryIdCount, option)) =>
        categoryIdCount.payCount = option.getOrElse(0L)
        (cid, categoryIdCount)
    }
  }

  def getPayCount(sessionId2FilterActionRDD: RDD[(String, UserVisitAction)]): RDD[(Long, Long)] = {
    sessionId2FilterActionRDD
      .filter(item => StringUtils.isNotEmpty(item._2.pay_category_ids))
      .flatMap(_._2.pay_category_ids.split(",").map(payCid => (payCid.toLong, 1L)))
      .reduceByKey(_+_)
  }

  def getOrderCount(sessionId2FilterActionRDD: RDD[(String, UserVisitAction)]): RDD[(Long, Long)] = {
    sessionId2FilterActionRDD
      .filter(item => StringUtils.isNotEmpty(item._2.order_category_ids))
      .flatMap(_._2.order_category_ids.split(",").map(orderCid => (orderCid.toLong, 1L)))
      .reduceByKey(_+_)
  }

  def getClickCount(sessionId2FilterActionRDD: RDD[(String, UserVisitAction)]): RDD[(Long, Long)] = {
    sessionId2FilterActionRDD
      .filter(item => item._2.click_category_id != -1L)
      .map(item => (item._2.click_category_id, 1L))
      .reduceByKey(_+_)
  }

  def top10PopularCategories(spark: SparkSession, taskUUID: String, sessionId2FilterActionRDD: RDD[(String, UserVisitAction)]): Array[(SortKey, Long)] = {
    val cid2CidRDD: RDD[(Long, Long)] = sessionId2FilterActionRDD.flatMap {
      case (_, action) =>
        val categoryBuffer = new ArrayBuffer[(Long, Long)]()

        if (action.click_category_id != -1) {
          categoryBuffer += ((action.click_category_id, action.click_category_id))
        } else if (StringUtils.isNotEmpty(action.order_category_ids)) {
          for (orderCid <- action.order_category_ids.split(","))
            categoryBuffer += ((orderCid.toLong, orderCid.toLong))
        } else if (StringUtils.isNotEmpty(action.pay_category_ids)) {
          for (payCid <- action.pay_category_ids.split(","))
            categoryBuffer += ((payCid.toLong, payCid.toLong))
        }
        categoryBuffer
    }.distinct()

    val cid2ClickCountRDD: RDD[(Long, Long)] = this.getClickCount(sessionId2FilterActionRDD)
    val cid2OrderCountRDD: RDD[(Long, Long)] = this.getOrderCount(sessionId2FilterActionRDD)
    val cid2PayCountRDD: RDD[(Long, Long)] = this.getPayCount(sessionId2FilterActionRDD)

    val cid2FullCountRDD: RDD[(Long, CategoryIdCount)] = this.getFullCount(cid2CidRDD, cid2ClickCountRDD, cid2OrderCountRDD, cid2PayCountRDD)

    val top10CategoryArray = cid2FullCountRDD.map {
      case (_, categoryIdCount) =>
        val sortKey = SortKey(categoryIdCount.clickCount, categoryIdCount.orderCount, categoryIdCount.payCount)
        (sortKey, categoryIdCount.categoryId)
    }.sortByKey(ascending = false).take(10)

    val top10CategoryRDD: RDD[Top10Category] = spark.sparkContext.makeRDD(top10CategoryArray)
      .map(item => Top10Category(taskUUID, item._2, item._1.clickCount, item._1.orderCount, item._1.payCount))

    import spark.implicits._
    top10CategoryRDD.toDF().write
      .format("jdbc")
      .option("url", ConfigurationManager.config.getString(Constants.JDBC_URL))
      .option("user", ConfigurationManager.config.getString(Constants.JDBC_USER))
      .option("password", ConfigurationManager.config.getString(Constants.JDBC_PASSWORD))
      .option("dbtable", "top10_category")
      .mode(SaveMode.Append)
      .save()

    top10CategoryArray
  }

  def generateRandomIndexList(extractPerDay: Int, daySessionCount: Long, hourCountMap: mutable.HashMap[String, Long],
                              hourListMap: mutable.HashMap[String, ListBuffer[Int]]): Unit = {
    for ((hour, count) <- hourCountMap) {
      // 计算一个小时要抽取多少条数据：(该小时总共的session个数 / 当天的session个数) * 一天要抽取多少条
      var hourExrCount = (count / daySessionCount.toInt) * extractPerDay
      if (hourExrCount > count) {
        hourExrCount = count.toInt
      }

      val random = new Random()
      hourListMap.get(hour) match {
        case None => hourListMap(hour) = new ListBuffer[Int]
          for (_ <- 0 until hourExrCount.toInt) {
            var index = random.nextInt(count.toInt)
            while (hourListMap(hour).contains(index)) {
              index = random.nextInt(count.toInt)
            }
            hourListMap(hour).append(index)
          }
        case Some(_) =>
          for (_ <- 0 until hourExrCount.toInt) {
            var index = random.nextInt(count.toInt)
            while (hourListMap(hour).contains(index)) {
              index = random.nextInt(count.toInt)
            }
            hourListMap(hour).append(index)
          }
      }
    }
  }

  def sessionRandomExtract(spark: SparkSession, taskUUID: String, session2FilterRDD: RDD[(String, FullInfo)]): Unit = {
    val dateHour2FullInfoRDD: RDD[(String, FullInfo)] = session2FilterRDD.map {
      case (_, fullInfo) =>
        val startTime = fullInfo.aggrInfo.start_time
        // dateHour: yyyy-MM-dd_HH
        val dateHour = DateUtils.getDateHour(startTime)
        (dateHour, fullInfo)
    }

    // hourCountMap: Map[(dateHour, count)]
    val hourCountMap: collection.Map[String, Long] = dateHour2FullInfoRDD.countByKey()

    // dateHourCountMap: Map[(date, Map[(hour, count)])]
    val dateHourCountMap = new mutable.HashMap[String, mutable.HashMap[String, Long]]()

    for ((dateHour, count) <- hourCountMap) {
      val date = dateHour.split("_")(0)
      val hour = dateHour.split("_")(1)

      dateHourCountMap.get(date) match {
        case None => dateHourCountMap(date) = new mutable.HashMap[String, Long]()
          dateHourCountMap(date) += (hour -> count)
        case Some(_) => dateHourCountMap(date) += (hour -> count)
      }
    }

    /*
     * 解决问题：
     *  一共有多少天：dateHourCountMap.size
     *  一天要抽取多少条：100 / dateHourCountMap.size
     */
    val extractPerDay = 100 / dateHourCountMap.size

    /*
     * 解决问题：
     *  某一天有多少session：dateHourCountMap(date).values.sum
     *  某一小时有多少session：dateHourCountMap(date)(hour)
     */
    val dateHourExtractIndexListMap = new mutable.HashMap[String, mutable.HashMap[String, ListBuffer[Int]]]()

    for ((date, hourCountMap) <- dateHourCountMap) {
      val daySessionCount = hourCountMap.values.sum
      dateHourExtractIndexListMap.get(date) match {
        case None => dateHourExtractIndexListMap(date) = new mutable.HashMap[String, ListBuffer[Int]]()
          this.generateRandomIndexList(extractPerDay, daySessionCount, hourCountMap, dateHourExtractIndexListMap(date))
        case Some(_) =>
          this.generateRandomIndexList(extractPerDay, daySessionCount, hourCountMap, dateHourExtractIndexListMap(date))
      }

      val dateHourExtractIndexListMapBd = spark.sparkContext.broadcast(dateHourExtractIndexListMap)

      val extractSessionRDD: RDD[SessionRandomExtract] = dateHour2FullInfoRDD.groupByKey().flatMap {
        case (dateHour, iterableFullInfo) =>
          val date = dateHour.split("_")(0)
          val hour = dateHour.split("_")(1)

          val extractList = dateHourExtractIndexListMapBd.value(date)(hour)
          val extractSessionArrayBuffer = new ArrayBuffer[SessionRandomExtract]()

          var index = 0
          for (fullInfo <- iterableFullInfo) {
            if (extractList.contains(index)) {
              val sessionId = fullInfo.aggrInfo.session_id
              val startTime = fullInfo.aggrInfo.start_time
              val searchKeywords = fullInfo.aggrInfo.search_keywords
              val clickCategoryIds = fullInfo.aggrInfo.click_category_id

              val extractSession = SessionRandomExtract(taskUUID, sessionId, startTime, searchKeywords, clickCategoryIds)
              extractSessionArrayBuffer += extractSession
            }
            index += 1
          }
          extractSessionArrayBuffer
      }

      import spark.implicits._
      extractSessionRDD.toDF().write
        .format("jdbc")
        .option("url", ConfigurationManager.config.getString(Constants.JDBC_URL))
        .option("user", ConfigurationManager.config.getString(Constants.JDBC_USER))
        .option("password", ConfigurationManager.config.getString(Constants.JDBC_PASSWORD))
        .option("dbtable", "session_extract")
        .mode(SaveMode.Append)
        .save()
    }
  }

  def saveSessionRatio(spark: SparkSession, taskUUID: String, value: mutable.HashMap[String, Int]): Unit = {
    val session_count = value.getOrElse(Constants.SESSION_COUNT, 1).toDouble

    val visitLength_1s_3s = value.getOrElse(Constants.TIME_PERIOD_1s_3s, 0)
    val visitLength_4s_6s = value.getOrElse(Constants.TIME_PERIOD_4s_6s, 0)
    val visitLength_7s_9s = value.getOrElse(Constants.TIME_PERIOD_7s_9s, 0)
    val visitLength_10s_30s = value.getOrElse(Constants.TIME_PERIOD_10s_30s, 0)
    val visitLength_1m_3m = value.getOrElse(Constants.TIME_PERIOD_1m_3m, 0)
    val visitLength_3m_10m = value.getOrElse(Constants.TIME_PERIOD_3m_10m, 0)
    val visitLength_10m_30m = value.getOrElse(Constants.TIME_PERIOD_10m_30m, 0)

    val stepLength_1_3 = value.getOrElse(Constants.STEP_PERIOD_1_3, 0)
    val stepLength_4_6 = value.getOrElse(Constants.STEP_PERIOD_4_6, 0)
    val stepLength_7_9 = value.getOrElse(Constants.STEP_PERIOD_7_9, 0)
    val stepLength_10_30 = value.getOrElse(Constants.STEP_PERIOD_10_30, 0)
    val stepLength_30_60 = value.getOrElse(Constants.STEP_PERIOD_30_60, 0)

    val visitLength_1s_3s_ration = (visitLength_1s_3s / session_count).formatted("%.2f").toDouble
    val visitLength_4s_6s_ration = (visitLength_4s_6s / session_count).formatted("%.2f").toDouble
    val visitLength_7s_9s_ration = (visitLength_7s_9s / session_count).formatted("%.2f").toDouble
    val visitLength_10s_30s_ration = (visitLength_10s_30s / session_count).formatted("%.2f").toDouble
    val visitLength_1m_3m_ration = (visitLength_1m_3m / session_count).formatted("%.2f").toDouble
    val visitLength_3m_10m_ration = (visitLength_3m_10m / session_count).formatted("%.2f").toDouble
    val visitLength_10m_30m_ration = (visitLength_10m_30m / session_count).formatted("%.2f").toDouble

    val stepLength_1_3_ration = (stepLength_1_3 / session_count).formatted("%.2f").toDouble
    val stepLength_4_6_ration = (stepLength_4_6 / session_count).formatted("%.2f").toDouble
    val stepLength_7_9_ration = (stepLength_7_9 / session_count).formatted("%.2f").toDouble
    val stepLength_10_30_ration = (stepLength_10_30 / session_count).formatted("%.2f").toDouble
    val stepLength_30_60_ration = (stepLength_30_60 / session_count).formatted("%.2f").toDouble

    val stat = SessionAggrStat(taskUUID, session_count.toLong, visitLength_1s_3s_ration,
      visitLength_4s_6s_ration, visitLength_7s_9s_ration, visitLength_10s_30s_ration,
      visitLength_1m_3m_ration, visitLength_3m_10m_ration, visitLength_10m_30m_ration,
      stepLength_1_3_ration, stepLength_4_6_ration, stepLength_7_9_ration,
      stepLength_10_30_ration, stepLength_30_60_ration)
    val sessionRationRDD = spark.sparkContext.makeRDD(Array(stat))

    import spark.implicits._
    sessionRationRDD.toDF().write
      .format("jdbc")
      .option("url", ConfigurationManager.config.getString(Constants.JDBC_URL))
      .option("user", ConfigurationManager.config.getString(Constants.JDBC_USER))
      .option("password", ConfigurationManager.config.getString(Constants.JDBC_PASSWORD))
      .option("dbtable", "session_stat_ratio")
      .mode(SaveMode.Append)
      .save()
  }

  def calculateStepLength(stepLength: Long, sessionAccumulator: SessionAccumulator): Unit = {
    if (stepLength >= 1 && stepLength <= 3) {
      sessionAccumulator.add(Constants.STEP_PERIOD_1_3)
    } else if (stepLength >= 4 && stepLength <= 6) {
      sessionAccumulator.add(Constants.STEP_PERIOD_4_6)
    } else if (stepLength >= 7 && stepLength <= 9) {
      sessionAccumulator.add(Constants.STEP_PERIOD_7_9)
    } else if (stepLength >= 10 && stepLength <= 30) {
      sessionAccumulator.add(Constants.STEP_PERIOD_10_30)
    } else if (stepLength > 30 && stepLength <= 60) {
      sessionAccumulator.add(Constants.STEP_PERIOD_30_60)
    }
  }

  def calculateVisitLength(visitLength: Long, sessionAccumulator: SessionAccumulator): Unit = {
    if (visitLength >= 1 && visitLength <= 3) {
      sessionAccumulator.add(Constants.TIME_PERIOD_1s_3s)
    } else if (visitLength >= 4 && visitLength <= 6) {
      sessionAccumulator.add(Constants.TIME_PERIOD_4s_6s)
    } else if (visitLength >= 7 && visitLength <= 9) {
      sessionAccumulator.add(Constants.TIME_PERIOD_7s_9s)
    } else if (visitLength >= 10 && visitLength <= 30) {
      sessionAccumulator.add(Constants.TIME_PERIOD_10s_30s)
    } else if (visitLength > 60 && visitLength <= 180) {
      sessionAccumulator.add(Constants.TIME_PERIOD_1m_3m)
    } else if (visitLength > 180 && visitLength <= 600) {
      sessionAccumulator.add(Constants.TIME_PERIOD_3m_10m)
    } else if (visitLength > 600 && visitLength <= 1800) {
      sessionAccumulator.add(Constants.TIME_PERIOD_10m_30m)
    }
  }

  /**
   * 按照配置文件中的过滤条件过滤RDD中的数据
   * @param taskParam             配置文件中的参数
   * @param sessionId2FullInfoRDD 过滤后的session聚合信息RDD
   */
  def getSessionFilteredRDD(spark: SparkSession, taskParam: JSONObject,
                            sessionId2FullInfoRDD: RDD[(String, FullInfo)]): (SessionAccumulator, RDD[(String, FullInfo)]) = {
    val startAge = ParamUtils.getParam(taskParam, Constants.PARAM_START_AGE)
    val endAge = ParamUtils.getParam(taskParam, Constants.PARAM_END_AGE)
    val professionals = ParamUtils.getParam(taskParam, Constants.PARAM_PROFESSIONALS)
    val cities = ParamUtils.getParam(taskParam, Constants.PARAM_CITIES)
    val sex = ParamUtils.getParam(taskParam, Constants.PARAM_SEX)
    val keywords = ParamUtils.getParam(taskParam, Constants.PARAM_KEYWORDS)
    val categoryIds = ParamUtils.getParam(taskParam, Constants.PARAM_CATEGORY_IDS)

    val filterParam = FilterParam(startAge.toInt, endAge.toInt, professionals, cities, sex, keywords, categoryIds)
    val filterParamBC: Broadcast[FilterParam] = spark.sparkContext.broadcast(filterParam)

    val sessionAccumulator = new SessionAccumulator
    spark.sparkContext.register(sessionAccumulator)

    val sessionId2FilterRDD = sessionId2FullInfoRDD.filter {
      case (_, fullInfo) =>
        var success = true
        if (!NumberUtils.between(filterParamBC.value.start_age, filterParamBC.value.end_age, fullInfo.age)) {
          success = false
        } else if (StringUtils.isNotEmpty(filterParamBC.value.professionals) &&
          !StringUtils.in(filterParamBC.value.professionals.split(","), fullInfo.professional)) {
          success = false
        } else if (StringUtils.isNotEmpty(filterParamBC.value.cities) &&
          !StringUtils.in(filterParamBC.value.cities.split(","), fullInfo.city)) {
          success = false
        } else if (StringUtils.isNotEmpty(filterParamBC.value.sex) &&
          !StringUtils.equal(filterParamBC.value.sex, fullInfo.sex)) {
          success = false
        } else if (StringUtils.isNotEmpty(filterParamBC.value.keywords) &&
          !StringUtils.in(filterParamBC.value.keywords.split(","), fullInfo.aggrInfo.search_keywords)) {
          success = false
        } else if (StringUtils.isNotEmpty(filterParamBC.value.categoryIds) &&
          !StringUtils.in(filterParamBC.value.categoryIds.split(","), fullInfo.aggrInfo.click_category_id)) {
          success = false
        }

        if (success) {
          sessionAccumulator.add(Constants.SESSION_COUNT)
          val visitLength = fullInfo.aggrInfo.visit_length
          val stepLength = fullInfo.aggrInfo.step_Length

          this.calculateVisitLength(visitLength, sessionAccumulator)
          this.calculateStepLength(stepLength, sessionAccumulator)
        }
        success
    }
    (sessionAccumulator, sessionId2FilterRDD)
  }

  /**
   * 整合session完整信息RDD
   * @param spark                     SparkSession
   * @param sessionId2GroupActionRDD  session完整信息RDD
   * @return
   */
  def getSessionFullInfo(spark: SparkSession,
                         sessionId2GroupActionRDD: RDD[(String, Iterable[UserVisitAction])]): RDD[(String, FullInfo)] = {
    val userId2AggrInfoRDD: RDD[(Long, AggrInfo)] = sessionId2GroupActionRDD.map {
      case (sessionId, iterableAction) =>
        var userId = -1L

        var startTime: Date = null
        var endTime: Date = null

        var stepLength = 0

        val searchKeywords = new StringBuffer("")
        val clickCategories = new StringBuffer("")

        for (action <- iterableAction) {
          if (userId == -1L) {
            userId = action.user_id
          }

          val actionTime = DateUtils.parseTime(action.action_time)
          if (startTime == null || actionTime.before(startTime)) {
            startTime = actionTime
          }
          if (endTime == null || actionTime.after(endTime)) {
            endTime = actionTime
          }

          val searchKeyword = action.search_keywords
          if (StringUtils.isNotEmpty(searchKeyword) && !searchKeywords.toString.contains(searchKeyword)) {
            searchKeywords.append(searchKeyword + ",")
          }

          val clickCategoryId = action.click_category_id
          if (clickCategoryId != -1 && !clickCategories.toString.contains(clickCategoryId)) {
            clickCategories.append(clickCategoryId + ",")
          }

          stepLength += 1
        }

        val searchKw = StringUtils.trimComma(searchKeywords.toString)
        val clickCg = StringUtils.trimComma(clickCategories.toString)

        val visitLength = (endTime.getTime - startTime.getTime) / 1000

        (userId, AggrInfo(sessionId, searchKw, clickCg, visitLength, stepLength, DateUtils.formatTime(startTime)))
    }

    import spark.implicits._
    val userId2UserInfoRDD: RDD[(Long, UserInfo)] = spark.sql("select * from user_info")
      .as[UserInfo].rdd.map(item => (item.user_id, item))

    val sessionId2FullInfoRDD: RDD[(String, FullInfo)] = userId2AggrInfoRDD.join(userId2UserInfoRDD).map {
      case (_, (aggrInfo, userInfo)) =>
        val age = userInfo.age
        val professional = userInfo.professional
        val sex = userInfo.sex
        val city = userInfo.city

        (aggrInfo.session_id, FullInfo(aggrInfo, age, professional, sex, city))
    }
    sessionId2FullInfoRDD
  }

  /**
   * 查询动作表数据
   * @param spark     SparkSession
   * @param taskParam 筛选条件
   * @return
   */
  def getOriActionRDD(spark: SparkSession, taskParam: JSONObject): RDD[UserVisitAction] = {
    val startDate = ParamUtils.getParam(taskParam, Constants.PARAM_START_DATE)
    val endDate = ParamUtils.getParam(taskParam, Constants.PARAM_END_DATE)

    import spark.implicits._
    spark.sql(s"select * from user_visit_action where date >= '$startDate' and date <= '$endDate'")
      .as[UserVisitAction].rdd
  }
}
