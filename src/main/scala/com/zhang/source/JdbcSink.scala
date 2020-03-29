package com.zhang.source


import java.sql.{Connection, DriverManager, PreparedStatement}

import com.zhang.source.Sensor_ZiDingYi.MySensorSource
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}
import org.apache.flink.streaming.api.scala._

object JdbcSink {

  def main(args: Array[String]): Unit = {

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val dataStream = env.addSource(new MySensorSource())

    dataStream.addSink(new MyJdbcSink())
    env.execute("jdbc test")
  }
}

//自定义Sink Function
class MyJdbcSink() extends RichSinkFunction[SensorReading]{
  //定义一个链接
  var conn: Connection = _
  //定义预编译语句
  var insertStmt: PreparedStatement = _
  var updateStmt: PreparedStatement = _

  override def open(parameters: Configuration): Unit = {
    //创建连接和预编译器
    conn = DriverManager.getConnection("jdbc:mysql://hadoop102:3306/test",
      "root", "123456")
    //?表示占位符
    insertStmt = conn.prepareStatement("insert into temperatures (sensor, temp) values(?,?)")
    updateStmt = conn.prepareStatement("update temperatures set temp = ? where sensor = ?")
  }


  override def invoke(value: SensorReading, context: SinkFunction.Context[_]): Unit = {
    //直接执行更新语句，如果没有查询到，那么在执行插入语句
    //1,2分别表示占位符的位置
    updateStmt.setDouble(1,value.temperature)
    updateStmt.setString(2,value.id)
    updateStmt.execute()
    //如果没有更新，说明没有查询到对应的id，那么执行插入
    if(updateStmt.getUpdateCount == 0){
      insertStmt.setString(1,value.id)
      insertStmt.setDouble(2,value.temperature)
      insertStmt.execute()
    }
  }

  override def close(): Unit = {
    insertStmt.close()
    updateStmt.close()
    conn.close()
  }
}
