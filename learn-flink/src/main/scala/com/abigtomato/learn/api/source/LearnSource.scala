package com.albert.api.source

import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011

import scala.util.Random

case class SensorReading(id: String, timestamp: Long, temperature: Double)

object LearnSource {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    // 1.
    val collectionStream = env.fromCollection(List(
      SensorReading("sensor_1", 1547718199, 35.80018327300259),
      SensorReading("sensor_6", 1547718201, 15.402984393403084),
      SensorReading("sensor_7", 1547718202, 6.720945201171228),
      SensorReading("sensor_10", 1547718205, 38.101067604893444)
    ))
//    collectionStream.print("collectionStream").setParallelism(1)

    // 2.
    val elemStream = env.fromElements(1, 2.0, "3")
//    elemStream.print("elemStream").setParallelism(1)

    // 3.
    val fileStream = env.readTextFile("D:\\WorkSpace\\javaproject\\learn-flink\\src\\main\\resources\\sensor.csv")
//    fileStream.print("fileStream").setParallelism(1)

    // 4.
    val socketStream = env.socketTextStream("localhost", 7777)
//    socketStream.print("socketStream").setParallelism(1)

    // 5.
    val properties = new Properties()
    properties.setProperty("bootstrap.servers", "localhost:9092")
    properties.setProperty("group.id", "consumer-group")
    properties.setProperty("key.deserializer",
      "org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty("value.deserializer",
      "org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty("auto.offset.reset", "latest")
    val kafkaStream = env.addSource(new FlinkKafkaConsumer011[String]("sensor", new SimpleStringSchema(), properties))
//    kafkaStream.print("kafkaStream").setParallelism(1)

    // 6.
    val sensorStream = env.addSource(new SensorSource)
    sensorStream.print("sensorStream").setParallelism(1)

    env.execute("Sensor")
  }
}

class SensorSource() extends SourceFunction[SensorReading]{

  var running: Boolean = true

  override def run(sourceContext: SourceFunction.SourceContext[SensorReading]): Unit = {
    val rand = new Random()

    var curData = 1.to(10).map(i => ("sensor_" + i, 60 + rand.nextGaussian() * 20))

    while(running) {
      val curTime = System.currentTimeMillis()
      curData.foreach(d => sourceContext.collect(SensorReading(d._1, curTime, d._2)))

      curData = curData.map(d => (d._1, d._2 + rand.nextGaussian()))

      Thread.sleep(1000)
    }
  }

  override def cancel(): Unit = {
    running = false
  }
}
