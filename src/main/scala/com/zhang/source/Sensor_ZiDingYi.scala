package com.zhang.source

import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.scala._

import scala.collection.immutable
import scala.util.Random

object Sensor_ZiDingYi {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    //自定义数据源
    val stream: DataStream[SensorReading] = env.addSource(new MySensorSource())
    stream.print()
    env.execute()
  }

  //自定义的source function类
  class MySensorSource() extends SourceFunction[SensorReading]() {
    //先定义一个标识位，用于控制是否发送数据
    var running: Boolean = true

    override def cancel(): Unit = {

      running = false

    }

    //核心方法，如果running为true，不停的发数据
    override def run(sourceContext: SourceFunction.SourceContext[SensorReading]): Unit = {
      //初始化一个随机数发生器
      val rand = new Random()
      //随机生成10个传感器的初始温度值
      var curtemp: immutable.IndexedSeq[(String, Double)] = 1.to(10).map {
        i => ("sensor_" + i, 65 + rand.nextGaussian() * 20)
      }
      //如果没有cancel，无限循环产生数据
      while (running) {
        //更新温度值
        curtemp = curtemp.map {
          t => (t._1, t._2 + rand.nextGaussian())
        }
        //获取当前的时间戳
        val curTime: Long = System.currentTimeMillis()
        //把10个数据都添加上时间戳，全部输出
        curtemp.foreach(
          t => {
            sourceContext.collect(SensorReading(t._1, curTime, t._2))
          }
        )
      }
    }

  }

}
