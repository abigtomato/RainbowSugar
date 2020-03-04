package com.abigtomato.learn.actor

// Worker发送的所有消息高层封装
class WorkerInfo(val id: String, val workerHost: String, val memory: String, val cores: String) {

  // 消息的发送时间（用于判断消息发送是否超时）
  var lastHeartbeat: Long = System.currentTimeMillis()

  // 消息具体信息
  override def toString = s"WorkerInfo($id, $workerHost, $memory, $cores)"
}