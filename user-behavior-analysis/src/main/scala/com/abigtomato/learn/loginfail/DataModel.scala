package com.abigtomato.learn.loginfail

// 输入的登录事件样例类（用户id，ip地址，登录状态，登录时间）
case class LoginEvent(userId: Long, ip: String, eventType: String, eventTime: Long)

// 输出的异常报警信息样例类（用户id，第一次登录检查时间，最后一次登录检查时间，报警信息）
case class Warning(userId: Long, firstFailTime: Long, lastFailTime: Long, warningMsg: String)