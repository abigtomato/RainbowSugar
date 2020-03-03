package com.albert.api.table

import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011
import org.apache.flink.table.api._
import com.alibaba.fastjson.JSON

case class EcommerceLog(id: String, timestamp: Long, temperature: Double)

object LearnTable {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tableEnv = TableEnvironment.getTableEnvironment(env)

    val properties = new Properties()
    properties.setProperty("bootstrap.servers", "192.168.121.100:9092")
    properties.setProperty("group.id", "consumer-group")
    properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty("auto.offset.reset", "latest")
    val dstream = env.addSource(new FlinkKafkaConsumer011[String]("ECOMMERCE", new SimpleStringSchema(), properties))

    val ecommerceLogDstream = dstream.map(jsonStr => {
      JSON.parseObject(jsonStr, classOf[EcommerceLog])
    })

//    val ecommerceLogTable: Table = tableEnv.fromDataStream(ecommerceLogDstream)
//    val table: Table = ecommerceLogTable.select("mid,ch").filter("ch='appstore'")
//    val midchDataStream: DataStream[(String, String)] = table.toAppendStream[(String, String)]

//    midchDataStream.print()
    env.execute()
  }
}
