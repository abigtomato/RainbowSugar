package com.abigtomato.learn.api.sink

import com.abigtomato.learn.api.source.SensorReading

import java.util
import org.apache.flink.api.common.functions.RuntimeContext
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.elasticsearch.{ElasticsearchSinkFunction, RequestIndexer}
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink
import org.apache.http.HttpHost
import org.elasticsearch.client.Requests

object LearnEsSink {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val fileStream = env.readTextFile("D:\\WorkSpace\\javaproject\\learn-flink\\src\\main\\resources\\sensor.csv")

    val dataStream = fileStream.map(data => {
      val dataArr = data.split(",")
      SensorReading(dataArr(0).trim, dataArr(1).trim.toLong, dataArr(2).trim.toDouble)
    })

    val httpHosts = new util.ArrayList[HttpHost]()
    httpHosts.add(new HttpHost("192.168.121.100", 9200))

    val esSinkBuilder = new ElasticsearchSink.Builder[SensorReading](httpHosts, new ElasticsearchSinkFunction[SensorReading] {
        override def process(t: SensorReading, runtimeContext: RuntimeContext, requestIndexer: RequestIndexer): Unit = {
          println("saving data: " + t)

          val json = new util.HashMap[String, String]()
          json.put("sensor_id", t.id)
          json.put("temperature", t.temperature.toString)
          json.put("timestamp", t.timestamp.toString)

          val request = Requests.indexRequest()
            .index("sensor")
            .`type`("reading_data")
            .source(json)

          requestIndexer.add(request)
          println("")
        }
      }
    )
    dataStream.addSink(esSinkBuilder.build())

    env.execute("es sink")
  }
}
