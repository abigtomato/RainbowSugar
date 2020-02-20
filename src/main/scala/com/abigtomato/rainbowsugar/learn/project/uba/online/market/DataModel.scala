package com.abigtomato.rainbowsugar.learn.project.uba.online.market

// 输入的广告点击事件样例类（用户id，广告id，省份，城市，时间戳）
case class AdClickEvent(userId: Long, adId: Long, province: String, city: String, timestamp: Long)

// 按照省份统计的输出结果样例类（时间窗口闭合时间，省份，点击数量）
case class CountByProvince(windowEnd: String, province: String, count: Long)

// 输出的黑名单报警信息（用户id，广告id，报警信息）
case class BlackListWarning(userId: Long, adId: Long, msg: String)

// 输入数据样例类（用户id，用户行为，渠道，时间戳）
case class MarketingUserBehavior(userId: String, behavior: String, channel: String, timestamp: Long)

// 输出结果样例类（窗口开启时间，窗口关闭时间，渠道，行为，统计量）
case class MarketingViewCount(windowStart: String, windowEnd: String, channel: String, behavior: String, count: Long)