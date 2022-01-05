package com.flink.demo.cases.case01

import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment, _}
import org.apache.flink.streaming.api.{CheckpointingMode, TimeCharacteristic}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010

/**
  * Created by DebugSy on 2018/6/8.
  *
  * case01:
  * 将kafka作为source，提取记录的时间戳，统计5秒内同一个id出现的次数，时间窗口时10s
  *
  * 输入数据：a 1
  *           a 2
  *           b 1
  * 输出数据：
  *
  */
object FlinkKafka010Source {

  val topic: String = "flink_1217"

  val bootstrap: String = "192.168.1.82:9092"

  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.createLocalEnvironment()
    env.enableCheckpointing(5000, CheckpointingMode.EXACTLY_ONCE)

    //kafka config
    val props = new Properties()
    props.setProperty("bootstrap.servers", bootstrap)
    props.setProperty("group.id", "shiy_test_source")
    props.put("enable.auto.commit", "false")

    //create kafka source
    val kafkaSource = new FlinkKafkaConsumer010[String](topic, new SimpleStringSchema(), props)
        .setStartFromLatest()//设置读取offset的起始位置

    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)//设置

    val source: DataStream[String] = env.addSource(kafkaSource).setParallelism(1)

    source.printToErr()

    env.execute("flink kafka source")
  }

}