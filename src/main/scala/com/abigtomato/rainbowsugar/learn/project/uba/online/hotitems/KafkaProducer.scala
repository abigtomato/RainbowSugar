package com.abigtomato.rainbowsugar.learn.project.uba.online.hotitems

import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

object KafkaProducer {

  def main(args: Array[String]): Unit = {
    writeToKafka("hotitems")
  }

  def writeToKafka(topic: String): Unit = {
    val properties = new Properties()
    properties.setProperty("bootstrap.servers", "192.168.121.100:9092")
    properties.setProperty("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    properties.setProperty("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

    val producer = new KafkaProducer[String, String](properties)
//    val bufferedSource = io.Source.fromFile("src/main/resources/UserBehavior.csv")
//    for (elem <- bufferedSource.getLines()) {
//      val record = new ProducerRecord[String, String](topic, elem)
//      producer.send(record)
//    }

    producer.close()
  }
}
