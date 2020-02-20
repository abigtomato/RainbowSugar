package com.abigtomato.rainbowsugar.learn.project.uba.offline.analysis.area

import com.abigtomato.rainbowsugar.learn.project.uba.offline.utils.StringUtils
import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.{DataType, StringType, StructField, StructType}

/**
 * 自定义聚合，合并，去重功能的UDAF
 */
class GroupConcatDistinct extends UserDefinedAggregateFunction {

  // 指定输入的数据类型
  override def inputSchema: StructType = StructType(StructField("cityInfo", StringType)::Nil)

  // 指定缓冲区的数据类型
  override def bufferSchema: StructType = StructType(StructField("bufferCityInfo", StringType)::Nil)

  // 指定输出的数据类型
  override def dataType: DataType = StringType

  // 保证输入输出数据一致
  override def deterministic: Boolean = true

  // 缓冲区初始化
  override def initialize(buffer: MutableAggregationBuffer): Unit = buffer(0) = ""

  // 更新缓冲区操作
  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    var bufferCityInfo = buffer.getString(0)
    val cityInfo = input.getString(0)

    if (!bufferCityInfo.contains(cityInfo)) {
      if (StringUtils.isEmpty(bufferCityInfo)) {
        bufferCityInfo += cityInfo
      } else {
        bufferCityInfo += "," + cityInfo
      }
      buffer.update(0, bufferCityInfo)
    }
  }

  // 两个缓存区的合并操作
  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    var bufferCityInfo1 = buffer1.getString(0)
    val bufferCityInfo2 = buffer2.getString(0)

    for (cityInfo <- bufferCityInfo2.split(",")) {
      if (!bufferCityInfo1.contains(cityInfo)) {
        if (StringUtils.isEmpty(bufferCityInfo1)) {
          bufferCityInfo1 += cityInfo
        } else {
          bufferCityInfo1 += "," + cityInfo
        }
      }
    }
    buffer1.update(0, bufferCityInfo1)
  }

  // UADF函数统计结果
  override def evaluate(buffer: Row): Any = buffer.getString(0)
}
