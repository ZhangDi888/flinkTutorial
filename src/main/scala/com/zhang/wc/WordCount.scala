package com.zhang.wc

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala._

object WordCount {
  def main(args: Array[String]): Unit = {
    //从外部传入参数
    //    val params: ParameterTool = ParameterTool.fromArgs(args)
    //    val host: String = params.get("host")
    //    val port: Int = params.getInt("port")

    //创建流处理执行环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    //创建流处理执行环境
    //    val textDataStream: DataStream[String] = env.socketTextStream("localhost", 7777)
    val inputPath = "D:\\ideaworkspace\\flinkTutorial\\src\\main\\resources\\hello.txt"
    val value: DataStream[String] = env.readTextFile(inputPath)
    //进行转换处理
    val dataStream: DataStream[(String, Int)] = value
      .flatMap(_.split(" "))
      .map((_, 1))
      .keyBy(0) //0代表元组的第一个索引
      .sum(1) //1代表元组的第二个索引

    dataStream.print()
    //启动任务
    env.execute()
  }
}
