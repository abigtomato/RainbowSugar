package com.abigtomato.rainbowsugar.learn.project.uba.offline.analysis.area

import java.util.UUID

import com.abigtomato.rainbowsugar.learn.project.uba.offline.conf.ConfigurationManager
import com.abigtomato.rainbowsugar.learn.project.uba.offline.constant.Constants
import com.abigtomato.rainbowsugar.learn.project.uba.offline.model.{AreaTop3Product, CityAreaInfo, CityClickProduct}
import com.abigtomato.rainbowsugar.learn.project.uba.offline.utils.ParamUtils
import net.sf.json.JSONObject
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SaveMode, SparkSession}

/**
 * 统计各个区域中top3的热门商品：
 *  - 1.热门商品的评判指标是商品的被点击次数
 *  - 2.对于user_visit_action表，click_product_id表示被点击的商品
 */
object AreaTop3Statistic {

  def main(args: Array[String]): Unit = {
    val jsonStr = ConfigurationManager.config.getString(Constants.TASK_PARAMS)
    val taskParam = JSONObject.fromObject(jsonStr)

    val taskUUID = UUID.randomUUID().toString

    val conf = new SparkConf().setMaster("local").setAppName("AreaTop3Statistic")
    val spark = SparkSession.builder().config(conf).enableHiveSupport().getOrCreate()

    // 获取城市和商品信息
    val cityId2PidRDD: RDD[(Long, Long)] = this.getCityAndProductInfo(spark, taskParam)
    // 获取城市和区域信息
    val cityId2AreaInfoRDD: RDD[(Long, CityAreaInfo)] = this.getCityAreaInfo(spark)
    // 创建城市，区域和商品的信息表
    this.createAreaPidBasicInfoTable(spark, cityId2PidRDD, cityId2AreaInfoRDD)

    // 统计区域和对应商品点击次数的结果表
    spark.udf.register("concat_long_string", (v1: Long, v2: String, split: String) => v1 + split + v2)
    spark.udf.register("group_concat_distinct", new GroupConcatDistinct)
    this.createAreaClickCountTable(spark)

    // 统计区域和商品信息对应的点击次数结果表
    spark.udf.register("get_json_field", (json: String, field: String) => {
      val jsonObject = JSONObject.fromObject(json)
      jsonObject.getString(field)
    })
    this.createAreaCountProductInfoTable(spark)

    // 统计各区域点击次数top3的商品
    this.createTop3Product(spark, taskUUID)

    spark.close()
  }

  def createTop3Product(spark: SparkSession, taskUUID: String): Unit = {
    val sql =
      s"SELECT " +
      s"  area, " +
      s"  CASE " +
      s"    WHEN area = '华北' OR area = '华东' THEN 'A_Level' " +
      s"    WHEN area = '华中' OR area = '华南' THEN 'B_Level' " +
      s"    WHEN area = '西南' OR area = '西北' THEN 'C_Level' " +
      s"    ELSE 'D_Level' " +
      s"  END area_level, " +
      s"  city_infos, pid, product_name, product_status, click_count " +
      s"FROM ( " +
      s"  SELECT " +
      s"    area, city_infos, pid, product_name, product_status, click_count, " +
      s"    row_number() over(PARTITION BY area ORDER BY click_count DESC) rank " +
      s"  FROM " +
      s"    temp_area_count_product_info" +
      s") as tmp WHERE rank <= 3"
    val top3ProductRDD: RDD[AreaTop3Product] = spark.sql(sql).rdd.map(row => AreaTop3Product(taskUUID, row.getAs[String]("area"),
      row.getAs[String]("area_level"), row.getAs[Long]("pid"),
      row.getAs[String]("city_infos"), row.getAs[Long]("click_count"),
      row.getAs[String]("product_name"), row.getAs[String]("product_status")))

    import spark.implicits._
    top3ProductRDD.toDF().write
      .format("jdbc")
      .option("url", ConfigurationManager.config.getString(Constants.JDBC_URL))
      .option("user", ConfigurationManager.config.getString(Constants.JDBC_USER))
      .option("password", ConfigurationManager.config.getString(Constants.JDBC_PASSWORD))
      .option("dbtable", "area_top3_product")
      .mode(SaveMode.Append)
      .save()
  }

  def createAreaCountProductInfoTable(spark: SparkSession): Unit = {
    val sql =
      s"SELECT " +
      s"  tacc.area, tacc.city_infos, tacc.pid, tacc.click_count, pi.product_name, " +
      s"  if(get_json_field(pi.extend_info, 'product_status') = '0', 'Self', 'Third Party') as product_status " +
      s"FROM " +
      s"  temp_area_click_count as tacc " +
      s"JOIN " +
      s"  product_info as pi " +
      s"ON " +
      s"  tacc.pid = pi.product_id"
    spark.sql(sql).createOrReplaceTempView("temp_area_count_product_info")
  }

  def createAreaClickCountTable(spark: SparkSession): Unit = {
    val sql =
      s"SELECT " +
      s"  area, pid, count(*) as click_count, " +
      s"  group_concat_distinct(concat_long_string(city_id, city_name, ',')) as city_infos " +
      s"FROM " +
      s"  temp_area_basic_info " +
      s"GROUP BY " +
      s"  area, pid"
    spark.sql(sql).createOrReplaceTempView("temp_area_click_count")
  }

  def createAreaPidBasicInfoTable(spark: SparkSession, cityId2PidRDD: RDD[(Long, Long)],
                                  cityId2AreaInfoRDD: RDD[(Long, CityAreaInfo)]): Unit = {
    import spark.implicits._
    cityId2PidRDD.join(cityId2AreaInfoRDD).map {
      case (cityId, (pid, areaInfo)) => (cityId, areaInfo.cityName, areaInfo.area, pid)
    }.toDF("city_id", "city_name", "area", "pid").createOrReplaceTempView("temp_area_basic_info")
  }

  def getCityAreaInfo(spark: SparkSession): RDD[(Long, CityAreaInfo)] = {
    val cityAreaInfoArray = Array(
      (0L, "北京", "华北"), (1L, "上海", "华东"), (2L, "南京", "华东"),
      (3L, "广州", "华南"), (4L, "三亚", "华南"), (5L, "武汉", "华中"),
      (6L, "长沙", "华中"), (7L, "西安", "西北"), (8L, "成都", "西南"),
      (9L, "哈尔滨", "东北"))

    spark.sparkContext.makeRDD(cityAreaInfoArray).map {
      case (cityId, cityName, area) => (cityId, CityAreaInfo(cityId, cityName, area))
    }
  }

  def getCityAndProductInfo(spark: SparkSession, taskParam: JSONObject): RDD[(Long, Long)] = {
    val startDate = ParamUtils.getParam(taskParam, Constants.PARAM_START_DATE)
    val endDate = ParamUtils.getParam(taskParam, Constants.PARAM_END_DATE)

    import spark.implicits._
    val sql =
      s"SELECT " +
      s"  click_id, click_product_id " +
      s"FROM " +
      s"  user_visit_action " +
      s"WHERE " +
      s"  date >= '$startDate' " +
      s"AND " +
      s"  date <= '$endDate' " +
      s"AND " +
      s"  click_product_id != -1"
    spark.sql(sql).as[CityClickProduct].rdd.map(cityPid => (cityPid.cityId, cityPid.clickProductId))
  }
}
