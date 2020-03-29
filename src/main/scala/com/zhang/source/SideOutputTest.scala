package com.zhang.source

import org.apache.flink.streaming.api.functions.{KeyedProcessFunction, ProcessFunction}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

object SideOutputTest {
  def main(args: Array[String]): Unit = {

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val inputDataStream: DataStream[String] = env.socketTextStream("hadoop102", 7777)
    val dataStream: DataStream[SensorReading] = inputDataStream.map(
      data => {
        val strings: Array[String] = data.split(",")
        SensorReading(strings(0).trim, strings(1).trim.toLong, strings(2).trim.toDouble)
      }
    )
    val splitData = dataStream.process(new SplitTempMonitor())
    splitData.print("high")
    splitData.getSideOutput(new OutputTag[SensorReading]("low-temp")).print("low")

    env.execute("process function test")
  }

}

class SplitTempMonitor() extends ProcessFunction[SensorReading,SensorReading]{
  override def processElement(value: SensorReading, context: ProcessFunction[SensorReading, SensorReading]#Context, collector: Collector[SensorReading]): Unit = {
    //判断温度，如果小于30就输出到侧输出流
    if(value.temperature < 30){
      context.output(new OutputTag[SensorReading]("low-temp"),value)
    } else {
      //30°以上的数据输出到主流
      collector.collect(value)
    }
  }
}
