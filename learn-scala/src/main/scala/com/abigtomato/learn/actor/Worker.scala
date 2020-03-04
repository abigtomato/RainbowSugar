package com.abigtomato.learn.actor

import java.util.UUID

import akka.actor.{Actor, ActorSelection, ActorSystem, Props}
import com.typesafe.config.ConfigFactory

import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global

/**
 * Worker节点
 */
class Worker extends Actor {
  // 维护Master的引用
  var master: ActorSelection = _

  // 随机产生Worker的ID
  val id: String = UUID.randomUUID().toString

  // 初始化
  override def preStart(): Unit = {
    // 主动连接Master
    master = context.system.actorSelection("akka.tcp://MasterActorSystem@localhost:8888/user/Master")

    // 发送注册消息
    master ! RegisterWorker(id, "localhost", "1024", "8")
  }

  // 接收消息
  override def receive: Receive = {
    // 接收到ACK后定时发送心跳
    case RegisteredWorker(workerId) => {
      context.system.scheduler.schedule(0 millis, 5000 millis, self, SendHeartBeat)
    }

    // 发送心跳
    case SendHeartBeat => {
      println("发送心跳消息")
      master ! HeartBeat(id)
    }
  }
}

object Worker {
  def main(args: Array[String]): Unit = {
    val clientPort = 8890

    val configStr =
      s"""
         |akka.actor.provider = "akka.remote.RemoteActorRefProvider"
         |akka.remote.netty.tcp.port = $clientPort
       """.stripMargin
    val config = ConfigFactory.parseString(configStr)

    val actorSystem = ActorSystem("WorkerActorSystem", config)
    actorSystem.actorOf(Props[Worker], "Worker")
  }
}