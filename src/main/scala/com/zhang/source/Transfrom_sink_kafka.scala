package com.zhang.source

import org.apache.flink.api.common.functions.RichFilterFunction
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011

object Transfrom_sink_kafka {

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
//    //滚动聚合,对不同区的相同的k的流进行聚合
//    val sumDS: DataStream[SensorReading] = dataDSMap.keyBy(0).sum(2)
//    val sumDS2: DataStream[SensorReading] = dataDSMap.keyBy("id").sum("temperature")
//    //分流
//    val splitStream: SplitStream[SensorReading] = dataDSMap.split {
//      SensorData => {
//        //大于30的分到高的流中
//        if (SensorData.temperature > 30) {
//          Seq("high")
//        } else {
//          //其他分到低的流
//          Seq("low")
//        }
//      }
//    }
//    //选择流
//    val highTempStream: DataStream[SensorReading] = splitStream.select("high")
//    val lowTempStream: DataStream[SensorReading] = splitStream.select("low")
//    //可以选择多个流
//    val allTempStream: DataStream[SensorReading] = splitStream.select("high", "low")
//    //警告流
//    val warningStream: DataStream[(String, Double, String)] = highTempStream.map(
//      data => {
//        (data.id, data.temperature, "high temperature warning")
//      }
//    )
    //连接流
//    val connectedStreams: ConnectedStreams[(String, Double, String), SensorReading] = warningStream.connect(lowTempStream)
//    val coMapStream: DataStream[Product] = connectedStreams.map(
//      warningData => (warningData._1, warningData._2),
//      lowData => (lowData.id, lowData.temperature, "healthy")
//    )
//    coMapStream.print()
//    println("--------------------------------------------")
//    val value: DataStream[SensorReading] = highTempStream.union(lowTempStream)
//    value.print()

    //函数类实例
//    val filterDS: DataStream[SensorReading] = dataDSMap.filter( new MyFilter("sensor_1") )
//    val filterMap: DataStream[String] = filterDS.map(
//      send => send.toString
//    )
//    //发送到kafka
//    filterMap.addSink(new FlinkKafkaProducer011[String]("hadoop102:9092","test",new SimpleStringSchema()))
    //匿名函数
//    val dataFilter: DataStream[SensorReading] = dataDSMap.filter(
//      new RichFilterFunction[SensorReading] {
//        override def filter(t: SensorReading): Boolean = {
//          t.id.contains("sensor_1")
//        }
//      }
//    )
//    dataFilter.print()
    env.execute()
  }

  class MyFilter(keyword: String) extends  RichFilterFunction[SensorReading]{
    override def filter(t: SensorReading): Boolean = {
      t.id.startsWith(keyword)
    }
  }

}
