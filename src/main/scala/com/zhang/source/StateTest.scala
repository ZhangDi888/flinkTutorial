package com.zhang.source

import java.util.concurrent.TimeUnit

import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.api.common.time.Time
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.environment.CheckpointConfig.ExternalizedCheckpointCleanup
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

object StateTest {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    //设置并行度
    env.setParallelism(1)
    //checkPoint设置
    //设置10秒为一个检查点
    env.enableCheckpointing(10000L)
    //设置精确一次
    env.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)
    //设置检查点，多久超时
    env.getCheckpointConfig.setCheckpointTimeout(60000L)
    //如果检查点存盘的过程中，出现了故障，算挂掉(默认为true)
    env.getCheckpointConfig.setFailOnCheckpointingErrors(true)
    //同时有几个检查点在进行
    env.getCheckpointConfig.setMaxConcurrentCheckpoints(2)
    //在两个检查点最小间隔(配置这个之后，setMaxConcurrentCheckpoints就失效了)
    env.getCheckpointConfig.setMinPauseBetweenCheckpoints(500L)
    //取消job的时候他的外部检查点是否保存(此处保存)
    env.getCheckpointConfig.enableExternalizedCheckpoints(ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION)

    //重启策略配置
    //固定时间重启次数,3为尝试重启的次数，10000L尝试重启的间隔时间
    env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 10000L))
    //失败率重启,2为重启次数，5为5分钟内，10为重启的时间间隔
    env.setRestartStrategy(RestartStrategies.failureRateRestart(2, Time.of(5, TimeUnit.MINUTES), Time.of(10, TimeUnit.SECONDS)))

    //状态后端配置
    //    env.setStateBackend(new RocksDBStateBackend(""))
    val inputStream: DataStream[String] = env.socketTextStream("hadoop102", 7777)
    val dataStream: DataStream[SensorReading] = inputStream.map(
      data => {
        val strings: Array[String] = data.split(",")
        SensorReading(strings(0).trim, strings(1).trim.toLong, strings(2).trim.toDouble)
      }
    )
    val dataStreamOut: DataStream[(String, Double, Double)] = dataStream.keyBy(_.id).flatMap(new TempChangeWarning(10.0))
    val dataStreamOut2 = dataStream.keyBy(_.id).flatMapWithState[(String, Double, Double),Double]({
      case (inputData: SensorReading, None) => (List.empty,Some(inputData.temperature))
      case (inputData: SensorReading, lastTemp: Some[Double]) =>
        //计算温度的差值
        val diff: Double = (inputData.temperature - lastTemp.get).abs
        if(diff > 10.0){
          (List((inputData.id,lastTemp.get,inputData.temperature)),Some(inputData.temperature))
        } else {
          (List.empty,Some(inputData.temperature))
        }
    })
    dataStreamOut2.print()

    env.execute("Rich FlatMap Function")
  }

}

class TempChangeWarning(threshold: Double) extends RichFlatMapFunction[SensorReading, (String, Double, Double)] {

  //定义一个状态用来保存上一次的温度值
  private var lastTempState: ValueState[Double] = _

  //对上次的温度值做初始化
  override def open(parameters: Configuration): Unit = {
    //new ValueStateDescriptor[Double]是一个描述器
    lastTempState = getRuntimeContext.getState(new ValueStateDescriptor[Double]("laet-temp", classOf[Double]))
  }


  override def flatMap(value: SensorReading, collector: Collector[(String, Double, Double)]): Unit = {
    //获取上次的温度值
    val lastTemp: Double = lastTempState.value()
    //计算差值
    val diff: Double = (value.temperature - lastTemp).abs
    //如果差值大于阈值，则输出
    if (diff > threshold) {
      collector.collect(value.id, lastTemp, value.temperature)
    }
    //更新状态
    lastTempState.update(value.temperature)
  }

}