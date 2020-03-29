package com.zhang.source

import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.redis.RedisSink
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig
import org.apache.flink.streaming.connectors.redis.common.mapper.{RedisCommand, RedisCommandDescription, RedisMapper}

object TransFrom_sink_redis1 {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val textDS: DataStream[String] = env.readTextFile("D:\\ideaworkspace\\flinkTutorial\\src\\main\\resources\\sensor.txt")

    //Tranfrom操作
    val dataDSMap: DataStream[SensorReading] = textDS.map {
      data => {
        val strings: Array[String] = data.split(",")
        SensorReading(strings(0).trim, strings(1).trim.toLong, strings(2).trim.toDouble)
      }
    }

    //函数类实例
//    val filterDS: DataStream[SensorReading] = dataDSMap.filter(new MyFilter("sensor_1"))

    val conf: FlinkJedisPoolConfig = new FlinkJedisPoolConfig
    .Builder()
      .setHost("hadoop102")
      .setPort(6379)
      .build()
    dataDSMap.addSink(new RedisSink[SensorReading](conf, new MyRedisMapper))

    env.execute()
  }


}

class MyRedisMapper extends RedisMapper[SensorReading] {
  override def getCommandDescription: RedisCommandDescription = {
    //保存到redis的命令，存成哈希表 hest
    new RedisCommandDescription(RedisCommand.HSET, "sensor_temp")
  }

  override def getKeyFromData(t: SensorReading): String = {
    t.temperature.toString
  }

  override def getValueFromData(t: SensorReading): String = {
    t.id.toString
  }
}