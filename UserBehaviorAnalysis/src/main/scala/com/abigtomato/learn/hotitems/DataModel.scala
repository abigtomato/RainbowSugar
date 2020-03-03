package com.abigtomato.learn.hotitems

/**
 * 输入数据的样例类
 * @param userId      用户id
 * @param itemId      商品id
 * @param categoryId  类别id
 * @param behavior    用户行为
 * @param timestamp   时间戳
 */
case class UserBehavior(userId: Long,
                        itemId: Long,
                        categoryId: Int,
                        behavior: String,
                        timestamp: Long)

/**
 * 窗口聚合结果样例类
 * @param itemId      商品id
 * @param windowEnd   窗口关闭时间
 * @param count       聚合数量
 */
case class ItemViewCount(itemId: Long,
                         windowEnd: Long,
                         count: Long)