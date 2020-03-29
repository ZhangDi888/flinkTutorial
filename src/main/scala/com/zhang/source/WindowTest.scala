package com.zhang.source

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

object WindowTest {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    //设置事件时间
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.getConfig.setAutoWatermarkInterval(100)

    val inputStream: DataStream[String] = env.socketTextStream("hadoop102",7777)

    val dataStream: DataStream[SensorReading] = inputStream.map {
      data =>
        val strings: Array[String] = data.split(",")
        SensorReading(strings(0).trim, strings(1).trim.toLong, strings(2).trim.toDouble)
    }//生成watermarks时，延迟生成数据的时间
      //对乱序数据分配时间戳和watermark
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[SensorReading](Time.seconds(1)) {
      override def extractTimestamp(t: SensorReading): Long = t.timestamp * 1000L//因为是毫秒级，要升级到表=秒所以*1000L
    })
    //统计每个传感器每15秒内的温度最小值
    val processedStream: DataStream[SensorReading] = dataStream
      .keyBy(_.id)
      .timeWindow(Time.seconds(15))
      .reduce((curMinTempData, newData) =>
          SensorReading(curMinTempData.id, newData.timestamp, newData.temperature.min(curMinTempData.temperature))
      )
    processedStream.print()

    env.execute("window test")
  }

}
