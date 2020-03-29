package com.zhang.source

import java.util

import org.apache.flink.api.common.functions.RuntimeContext
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.elasticsearch.{ElasticsearchSinkFunction, RequestIndexer}
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink
import org.apache.http.HttpHost
import org.elasticsearch.action.index.IndexRequest
import org.elasticsearch.client.{Request, Requests}

object Transfrom_sink_Es {
  def main(args: Array[String]): Unit = {

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val dataStream: DataStream[String] = env.readTextFile("D:\\ideaworkspace\\flinkTutorial\\src\\main\\resources\\sensor.txt")

    val dataStream2: DataStream[SensorReading] = dataStream.map(
      data => {
        val strings: Array[String] = data.split(",")
        SensorReading(strings(0).trim, strings(1).trim().toLong, strings(2).trim().toDouble)
      }
    )
    val httpHosts = new util.ArrayList[HttpHost]()
    httpHosts.add(new HttpHost("hadoop102", 9200))
    val esSinkBuilder: ElasticsearchSink.Builder[SensorReading] = new ElasticsearchSink.Builder[SensorReading](
      httpHosts,
      new ElasticsearchSinkFunction[SensorReading] {
        override def process(t: SensorReading, runtimeContext: RuntimeContext, requestIndexer: RequestIndexer): Unit = {
          //包装好要发送的数据
          val dataSource = new util.HashMap[String, String]()
          dataSource.put("sensor_id", t.id)
          dataSource.put("temperature", t.temperature.toString)
          dataSource.put("ts", t.timestamp.toString)
          //创建一个index request
          val indexReq = Requests.indexRequest()
            .index("sensor")
            .`type`("readingdata")
            //数据来源
            .source(dataSource)
          //用indexer发送请求
          requestIndexer.add(indexReq)
        }
      }
    )
    dataStream2.addSink(esSinkBuilder.build())

    env.execute()
  }
}
