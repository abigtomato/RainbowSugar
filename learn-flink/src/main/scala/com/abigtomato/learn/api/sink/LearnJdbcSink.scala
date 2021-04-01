package com.abigtomato.learn.api.sink

import com.abigtomato.learn.api.source.SensorReading

import java.sql.{Connection, DriverManager, PreparedStatement}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction
import org.apache.flink.streaming.api.scala._

object LearnJdbcSink {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val fileStream = env.readTextFile("D:\\WorkSpace\\javaproject\\learn-flink\\src\\main\\resources\\sensor.csv")

    val dataStream = fileStream.map(data => {
      val dataArr = data.split(",")
      SensorReading(dataArr(0).trim, dataArr(1).trim.toLong, dataArr(2).trim.toDouble)
    })

    dataStream.addSink(new RichSinkFunction[SensorReading] {

      var conn: Connection = _
      var insertStmt: PreparedStatement = _
      var updateStmt: PreparedStatement = _

      override def open(parameters: Configuration): Unit = {
        super.open(parameters)
        conn = DriverManager.getConnection("jdbc:mysql://192.168.121.100:3306/leyou", "root", "123456")
//        insertStmt = conn.prepareStatement("INSERT INTO `temperatures` (sensor, temp) VALUES (?, ?)")
//        updateStmt = conn.prepareStatement("UPDATE `temperatures` SET temp = ? WHERE sensor = ?")
      }

      override def invoke(value: SensorReading): Unit = {
        updateStmt.setDouble(1, value.temperature)
        updateStmt.setString(2, value.id)
        updateStmt.execute()

        if (updateStmt.getUpdateCount == 0) {
          insertStmt.setString(1, value.id)
          insertStmt.setDouble(2, value.temperature)
          insertStmt.execute()
        }
      }

      override def close(): Unit = {
        insertStmt.close()
        updateStmt.close()
        conn.close()
      }
    })

    env.execute("jdbc sink")
  }
}
