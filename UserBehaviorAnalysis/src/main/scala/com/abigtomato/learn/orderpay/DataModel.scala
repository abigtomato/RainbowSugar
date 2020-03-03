package com.abigtomato.learn.orderpay

// 定义输入订单事件的样例类
case class OrderEvent(orderId: Long, eventType: String, txId: String, eventTime: Long)

// 定义输出结果的样例类
case class OrderResult(orderId: Long, resultMsg: String)

// 输入的支付到账事件样例类（支付id，支付渠道，时间戳）
case class ReceiptEvent(txId: String, payChannel: String, eventTime: Long)