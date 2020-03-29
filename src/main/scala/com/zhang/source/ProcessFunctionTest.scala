package com.zhang.source

import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

object ProcessFunctionTest {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val inputStream: DataStream[String] = env.socketTextStream("hadoop102",7777)

    val dataStream: DataStream[SensorReading] = inputStream.map {
      data =>
        val strings: Array[String] = data.split(",")
        SensorReading(strings(0).trim, strings(1).trim.toLong, strings(2).trim.toDouble)
    }
    val processedStream: DataStream[String] = dataStream.keyBy(_.id).process(new TempIncreWarning())
    processedStream.print()

    env.execute()
  }

}
//自定义实现process function
//继承KeyedProcessFunction设定好输入，输出类型
class TempIncreWarning()extends KeyedProcessFunction[String,SensorReading,String] {
  //首先自定义状态，用来保存上一次的温度值，以及已经设定的定时时间戳
  //lazy懒加载，使用的时候才加载
  lazy val lastTempState: ValueState[Double] = getRuntimeContext.getState(new ValueStateDescriptor[Double]("last-temp",classOf[Double]))
  lazy val currentTimerState: ValueState[Long] = getRuntimeContext.getState(new ValueStateDescriptor[Long]("current-timer",classOf[Long]))
  override def processElement(value: SensorReading, context: KeyedProcessFunction[String, SensorReading, String]#Context, collector: Collector[String]): Unit = {
    //取出上次的温度值和定时器时间戳
    val lastTemp: Double = lastTempState.value()
    val curTimerTs: Long = currentTimerState.value()

    //将状态更新最新温度值
    lastTempState.update(value.temperature)

    //判断当前温度是否上升，如果上升而且没有注册定时器，那么注册10秒后
    if(value.temperature > lastTemp && curTimerTs == 0){
      //取出ProcessingTime + 10s
      val ts: Long = context.timerService().currentProcessingTime() + 10000L
      //注册定时器
      context.timerService().registerProcessingTimeTimer(ts)
      //保存时间戳到定时器
      currentTimerState.update(ts)
    } else if (value.temperature < lastTemp){
      //如果温度下降，那么删除定时器和状态
      context.timerService().deleteProcessingTimeTimer(curTimerTs)
      currentTimerState.clear()
    }
  }
  //能拿到当前的时间戳，上下文
  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[String, SensorReading, String]#OnTimerContext, out: Collector[String]): Unit = {
    out.collect("sensor" + ctx.getCurrentKey + "温度在10秒内连续上升")
    //已经报警了，保存的状态也就没必要存在了，清空，不然下次还会报警
    currentTimerState.clear()
  }
}
