package com.abigtomato.rainbowsugar.learn.project.uba.offline.model

/**
 * 用户行为样例类
 * @param date                行为产生日期
 * @param user_id             用户id
 * @param session_id          会话id
 * @param page_id             页面id
 * @param action_time         行为产生时间
 * @param search_keywords     搜索关键词
 * @param click_category_id   点击行为品类id
 * @param click_product_id    点击行为商品id
 * @param order_category_ids  下单行为品类id集合
 * @param order_product_ids   下单行为商品id集合
 * @param pay_category_ids    支付行为品类id集合
 * @param pay_product_ids     支付行为商品id集合
 * @param city_id             城市id
 */
case class UserVisitAction(date: String,
                           user_id: Long,
                           session_id: String,
                           page_id: Long,
                           action_time: String,
                           search_keywords: String,
                           click_category_id: Long,
                           click_product_id: Long,
                           order_category_ids: String,
                           order_product_ids: String,
                           pay_category_ids: String,
                           pay_product_ids: String,
                           city_id: Long)

/**
 * 用户信息样例类
 * @param user_id       用户id
 * @param username      用户名称
 * @param name          真实姓名
 * @param age           年龄
 * @param professional  职业
 * @param city          城市
 * @param sex           性别
 */
case class UserInfo(user_id: Long,
                    username: String,
                    name: String,
                    age: Int,
                    professional: String,
                    city: String,
                    sex: String)

/**
 * 商品信息样例类
 * @param product_id    商品id
 * @param product_name  商品名
 * @param extend_info   商品额外信息
 */
case class ProductInfo(product_id: Long, product_name: String, extend_info: String)

/**
 * 聚合信息样例类
 * @param session_id        会话id
 * @param search_keywords   搜索关键字
 * @param click_category_id 点击行为品类id
 * @param visit_length      会话时长
 * @param step_Length       会话步长
 * @param start_time        会话起始时间
 */
case class AggrInfo(session_id: String,
                    search_keywords: String,
                    click_category_id: String,
                    visit_length: Long,
                    step_Length: Long,
                    start_time: String)

/**
 * 完整信息样例类
 * @param aggrInfo          聚合信息样例类
 * @param age               年龄
 * @param professional      职业
 * @param sex               性别
 * @param city              城市
 */
case class FullInfo(aggrInfo: AggrInfo, age: Int, professional: String, sex: String, city: String)

/**
 * 过滤条件样例类
 * @param start_age
 * @param end_age
 * @param professionals
 * @param cities
 * @param sex
 * @param keywords
 * @param categoryIds
 */
case class FilterParam(start_age: Int,
                       end_age: Int,
                       professionals: String,
                       cities: String,
                       sex: String,
                       keywords: String,
                       categoryIds: String)

/**
 * session统计结果mysql表样例类
 * @param task_id
 * @param session_count
 * @param visit_length_1s_3s_ratio
 * @param visit_length_4s_6s_ratio
 * @param visit_length_7s_9s_ratio
 * @param visit_length_10s_30s_ratio
 * @param visit_length_1m_3m_ratio
 * @param visit_length_3m_10m_ratio
 * @param visit_length_10m_30m_ratio
 * @param step_length_1_3_ratio
 * @param step_length_4_6_ratio
 * @param step_length_7_9_ratio
 * @param step_length_10_30_ratio
 * @param step_length_30_60_ratio
 */
case class SessionAggrStat(task_id: String,
                           session_count: Long,
                           visit_length_1s_3s_ratio: Double,
                           visit_length_4s_6s_ratio: Double,
                           visit_length_7s_9s_ratio: Double,
                           visit_length_10s_30s_ratio: Double,
                           visit_length_1m_3m_ratio: Double,
                           visit_length_3m_10m_ratio: Double,
                           visit_length_10m_30m_ratio: Double,
                           step_length_1_3_ratio: Double,
                           step_length_4_6_ratio: Double,
                           step_length_7_9_ratio: Double,
                           step_length_10_30_ratio: Double,
                           step_length_30_60_ratio: Double)

/**
 * session随机抽取结果的样例类
 * @param taskId
 * @param sessionId
 * @param startTime
 * @param searchKeywords
 * @param clickCategoryIds
 */
case class SessionRandomExtract(taskId: String, sessionId: String, startTime: String, searchKeywords: String, clickCategoryIds: String)

/**
 * 品类id对应的统计结果
 * @param categoryId  品类id
 * @param clickCount  点击次数
 * @param orderCount  下单次数
 * @param payCount    支付次数
 */
case class CategoryIdCount(categoryId: Long, var clickCount: Long, var orderCount: Long, var payCount: Long)

/**
 * top10的品类统计结果
 * @param taskId
 * @param categoryId
 * @param clickCount
 * @param orderCount
 * @param payCount
 */
case class Top10Category(taskId: String, categoryId: Long, clickCount: Long, orderCount: Long, payCount: Long)

/**
 * top10的session统计结果
 * @param taskId
 * @param categoryId
 * @param sessionId
 * @param clickCount
 */
case class Top10Session(taskId: String, categoryId: Long, sessionId: String, clickCount: Long)

/**
 *
 * @param taskId
 * @param convertStr
 */
case class PageSplitConvertRate(taskId: String, convertStr: String)

/**
 * 各区域top3商品统计信息样例类
 * @param taskId        主键id
 * @param area          区域
 * @param areaLevel     区域等级
 * @param productId     商品id
 * @param cityInfos     城市信息
 * @param clickCount    点击次数
 * @param productName   商品名称
 * @param productStatus 商品类型
 */
case class AreaTop3Product(taskId: String,
                           area: String,
                           areaLevel: String,
                           productId: Long,
                           cityInfos: String,
                           clickCount: Long,
                           productName: String,
                           productStatus: String)

/**
 *
 * @param cityId
 * @param clickProductId
 */
case class CityClickProduct(cityId: Long, clickProductId: Long)

/**
 *
 * @param cityId
 * @param cityName
 * @param area
 */
case class CityAreaInfo(cityId: Long, cityName: String, area: String)