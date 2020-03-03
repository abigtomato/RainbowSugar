package com.albert.api.sink

import java.util.Properties

import com.albert.api.source.SensorReading
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer011, FlinkKafkaProducer011}

object LearnKafkaSink {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val properties = new Properties()
    properties.setProperty("bootstrap.servers", "192.168.121.100:9092")
    properties.setProperty("group.id", "consumer-group")
    properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty("auto.offset.reset", "latest")
    val kafkaStream = env.addSource(new FlinkKafkaConsumer011[String]("test", new SimpleStringSchema(), properties))

    val dataStream = kafkaStream.map(data => {
      val dataArr = data.split(",")
      SensorReading(dataArr(0).trim, dataArr(1).trim.toLong, dataArr(2).trim.toDouble).toString
    })

    dataStream.addSink(new FlinkKafkaProducer011[String]("192.168.121.100:9092", "sink_test", new SimpleStringSchema()))
    dataStream.print("kafka")

    env.execute("kafka sink")
  }
}
