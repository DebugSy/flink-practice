package com.flink.demo.table;

import com.flink.demo.common.WordCount;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

/**
 * Created by DebugSy on 2019/5/22.
 *
 * 测试案例：将表转换为回退模式的流
 * 注释部分是转换成附加模式的流
 */
public class TabletoRetractStream {

  public static void main(String[] args) throws Exception {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();
    StreamTableEnvironment tEnv = StreamTableEnvironment.getTableEnvironment(env);
    DataStream<WordCount> input = env.fromElements(
            new WordCount("a", 1),
            new WordCount("b", 1),
            new WordCount("c", 1),
            new WordCount("a", 1)
    );

    tEnv.registerDataStream("WordCount", input, "word, frequency");

//    Table table = tEnv.sqlQuery("select word,COUNT(frequency) as frequency FROM WordCount GROUP BY word");
//    DataStream<Tuple2<Boolean, Row>> tuple2DataStream = tEnv.toRetractStream(table, Row.class);
//    tuple2DataStream.print();

    Table table = tEnv.sqlQuery("select word,SUM(frequency) FROM WordCount GROUP BY word");
    DataStream<WordCount> wordCountDataStream = tEnv.toAppendStream(table, WordCount.class);
    wordCountDataStream.print();

    env.execute();
  }

}
