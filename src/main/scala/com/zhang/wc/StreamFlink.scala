package com.zhang.wc

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.scala._

object StreamFlink {
  def main(args: Array[String]): Unit = {
    //从外部传参
    val params: ParameterTool = ParameterTool.fromArgs(args)
    val host: String = params.get("host")
    val port: Int = params.getInt("port")

    //创建流处理执行环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    //从文本流读取流式数据
    val textDataStream: DataStream[String] = env.socketTextStream(host,port)
    //对流进行处理
    val dataStream: DataStream[(String, Int)] = textDataStream.flatMap(_.split(" ")).map((_, 1)).keyBy(0).sum(1)
    dataStream.print()
    env.execute()
  }
}
