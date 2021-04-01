package com.abigtomato.learn.api.sink

import com.abigtomato.learn.api.source.SensorReading
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.redis.RedisSink
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig
import org.apache.flink.streaming.connectors.redis.common.mapper.{RedisCommand, RedisCommandDescription, RedisMapper}

object LearnRedisSink {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val fileStream = env.readTextFile("D:\\WorkSpace\\javaproject\\learn-flink\\src\\main\\resources\\sensor.csv")

    val dataStream = fileStream.map(data => {
      val dataArr = data.split(",")
      SensorReading(dataArr(0).trim, dataArr(1).trim.toLong, dataArr(2).trim.toDouble)
    })

    val conf = new FlinkJedisPoolConfig.Builder()
      .setHost("192.168.121.100")
      .setPort(6379)
      .build()

    val redisSink = new RedisSink[SensorReading](conf, new RedisMapper[SensorReading] {
      override def getCommandDescription: RedisCommandDescription = new RedisCommandDescription(RedisCommand.HSET, "sensor")

      override def getKeyFromData(data: SensorReading): String = data.id.toString

      override def getValueFromData(data: SensorReading): String = data.temperature.toString
    })
    dataStream.addSink(redisSink)

    env.execute("redis sink")
  }
}
