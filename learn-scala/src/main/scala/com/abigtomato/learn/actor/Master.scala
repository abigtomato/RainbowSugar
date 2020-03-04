package com.abigtomato.learn.actor

import akka.actor.{Actor, ActorSystem, Props}
import com.typesafe.config.ConfigFactory

import scala.collection.mutable
import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global

/**
 * Master节点
 */
class Master extends Actor {
  // 在Master上注册的Worker的集合
  val workersMap = new mutable.HashMap[String, WorkerInfo]

  // 超时时间
  val WORKER_TIMEOUT: Int = 10 * 1000

  // 当Actor构造方法执行完执行一次
  override def preStart(): Unit = {
    // 每间隔一段时间给当前的Actor(Master)发送节点状态检测信息
    context.system.scheduler.schedule(5 millis, WORKER_TIMEOUT millis, self, CheckOfTimeOutWorker)
  }

  // 该方法会反复调用，用于接收消息，内部通过case模式匹配接收的消息类型
  override def receive: Receive = {
    // 节点注册消息
    case RegisterWorker(id, workerHost, memory, cores) =>
      if(!workersMap.contains(id)) {
        val worker = new WorkerInfo(id, workerHost, memory, cores)
        workersMap(id) = worker
        println("新注册的Worker节点: " + worker)
        // 发送ACK应答消息
        sender ! RegisteredWorker(worker.id)
      }

    // 节点状态检测消息
    case CheckOfTimeOutWorker =>
      val currentTime = System.currentTimeMillis()
      // 移除消息发送超时的Worker
      workersMap.values.filter(worker => {
        currentTime - worker.lastHeartbeat > WORKER_TIMEOUT
      }).foreach(worker => {
        workersMap.remove(worker.id)
      })
      println("Worker的数量: " + workersMap.keys.size)

    // 心跳消息
    case HeartBeat(workerId) =>
      val workerInfo = workersMap(workerId)
      println("接收 " + workerInfo + " 节点发送心跳消息")
      // 接收到心跳后重置用于超时计算的参数
      workerInfo.lastHeartbeat = System.currentTimeMillis()
  }
}

object Master {
  def main(args: Array[String]): Unit = {
    val host = "localhost"
    val port = 8888

    // akka的配置，第一条表示多个进程分布式运行，第二条第三条指定主机和端口
    val configStr =
      s"""
         |akka.actor.provider = "akka.remote.RemoteActorRefProvider"
         |akka.remote.netty.tcp.hostname = "$host"
         |akka.remote.netty.tcp.port = "$port"
       """.stripMargin
    val config = ConfigFactory.parseString(configStr)

    // 通过配置新建一个Actor
    val actorSystem = ActorSystem.create("MasterActorSystem", config)
    // 将Master类注册为一个名为Master的Actor
    actorSystem.actorOf(Props[Master], "Master")
  }
}