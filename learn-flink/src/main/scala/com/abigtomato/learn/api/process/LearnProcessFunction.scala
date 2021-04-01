package com.abigtomato.learn.api.process

import com.abigtomato.learn.api.source.SensorReading
import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

object LearnProcessFunction {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val socketStream = env.socketTextStream("localhost", 7777)

    val dataStream = socketStream.map(data => {
      val dataArr = data.split(",")
      SensorReading(dataArr(0).trim, dataArr(1).trim.toLong, dataArr(2).trim.toDouble)
    }).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[SensorReading](Time.seconds(1)) {
      override def extractTimestamp(element: SensorReading): Long = element.timestamp * 1000
    })

    // 若传感器温度连续10秒上升，则发出警报
    val processedStream = dataStream.keyBy(_.id)
      // 自定义的底层处理函数
      .process(new TempIncreAlert())

    // 若传感器前后温度差值为10，则发出警报（需要使用定时器等复杂功能，则使用这种方式）
    val processedStream2 = dataStream.keyBy(_.id)
      .process(new TempChangeAlert(10.0))

    // 若传感器前后温度差值为10，则发出警报（只需要使用状态编程，使用这种方式）
    val processedStream3 = dataStream.keyBy(_.id)
      .flatMap(new TempChangeAlert2(10.0))

    // 若传感器前后温度差值为10，则发出警报（第二种方式更简化的写法）
    val processedStream4 = dataStream.keyBy(_.id)
        // 有状态的flatMap算子，泛型[R: TypeInformation, S: TypeInformation]表示输出类型和状态类型
        .flatMapWithState[(String, Double, Double), Double]{
          // 若状态为None，也就是第一次处理数据，则初始化温度状态
          case (input: SensorReading, None) => (List.empty, Some(input.temperature))
          // 若存在状态
          case (input: SensorReading, lastTemp: Some[Double]) =>
            // 则从状态中获取上一次温度与本次温度作比较
            val diff = (input.temperature - lastTemp.get).abs
            // 大于阈值为输出报警信息，并更新温度状态
            if (diff > 10.0) {
              // 返回类型(TraversableOnce[R], Option[S])表示输出类型和状态类型
              (List((input.id, lastTemp.get, input.temperature)), Some(input.temperature))
            } else {
              (List.empty, Some(input.temperature))
            }
        }

    dataStream.print("dataStream")
//    processedStream.print("processedStream")
//    processedStream2.print("processedStream2")
//    processedStream3.print("processedStream3")
    processedStream4.print("processedStream4")

    env.execute("process function test")
  }
}

class TempIncreAlert() extends KeyedProcessFunction[String, SensorReading, String]{

  // 定义一个用于保存上一个数据温度值的状态
  lazy val lastTemp: ValueState[Double] = getRuntimeContext.getState(new ValueStateDescriptor[Double]("lastTemp", classOf[Double]))

  // 定义一个用于保存当前定时器时间戳的状态
  lazy val currentTimer: ValueState[Long] = getRuntimeContext.getState(new ValueStateDescriptor[Long]("currentTimer", classOf[Long]))

  // 处理函数
  override def processElement(value: SensorReading, ctx: KeyedProcessFunction[String, SensorReading, String]#Context, out: Collector[String]): Unit = {
    // 取出上一个数据的温度值
    val preTemp = lastTemp.value()
    // 更新为当前数据的温度值
    lastTemp.update(value.temperature)

    // 取出当前定时器的时间戳
    val curTimerTs = currentTimer.value()

    // 若相较于上一次温度上升，并且定时器的时间戳等于0（未设置定时器），则注册定时器
    if (value.temperature > preTemp && curTimerTs == 0) {
      // 设置定时器的触发时间戳为当前时间戳 + 10秒
      val timerTs = ctx.timerService().currentProcessingTime() + 10000L
      // 注册定时器
      ctx.timerService().registerProcessingTimeTimer(timerTs)
      // 更新当前定时器时间戳的状态
      currentTimer.update(timerTs)
    } else if (preTemp >= value.temperature || preTemp == 0.0) {
      // 若温度没有上升，或者当前真在处理第一条数据（preTemp等于0，上一个数据的温度为0），则删除定时器
      ctx.timerService().deleteProcessingTimeTimer(curTimerTs)
      // 并清空定时器的时间戳
      currentTimer.clear()
    }
  }

  // 定时器回调函数，若当前的时间戳和定时器中设置的时间戳相同，则调用该函数
  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[String, SensorReading, String]#OnTimerContext, out: Collector[String]): Unit = {
    out.collect(ctx.getCurrentKey + "：温度连续上升")  // 发出警报
    currentTimer.clear()  // 清空定时器
  }
}

class TempChangeAlert(threshold: Double) extends KeyedProcessFunction[String, SensorReading, (String, Double, Double)]{

  // 定义一个状态变量，保存上一次的温度值
  lazy val lastTempState: ValueState[Double] = getRuntimeContext.getState(new ValueStateDescriptor[Double]("lastTemp", classOf[Double]))

  override def processElement(value: SensorReading, ctx: KeyedProcessFunction[String, SensorReading, (String, Double, Double)]#Context, out: Collector[(String, Double, Double)]): Unit = {
    val lastTemp = lastTempState.value()
    // 取出上一次温度与本次温度对比，若大于阈值，则输出报警信息
    val diff = (value.temperature - lastTemp).abs
    if (diff > threshold) {
      out.collect((value.id, lastTemp, value.temperature))
    }
    // 更新状态变量
    lastTempState.update(value.temperature)
  }
}

class TempChangeAlert2(threshold: Double) extends RichFlatMapFunction[SensorReading, (String, Double, Double)]{

  private var lastTempState: ValueState[Double] = _

  // 通过类的生命周期函数操作状态
  override def open(parameters: Configuration): Unit = {
    lastTempState = getRuntimeContext.getState(new ValueStateDescriptor[Double]("lastTemp", classOf[Double]))
  }

  override def flatMap(value: SensorReading, out: Collector[(String, Double, Double)]): Unit = {
    val lastTemp = lastTempState.value()
    val diff = (value.temperature - lastTemp).abs
    if (diff > threshold) {
      out.collect((value.id, lastTemp, value.temperature))
    }
    lastTempState.update(value.temperature)
  }
}
