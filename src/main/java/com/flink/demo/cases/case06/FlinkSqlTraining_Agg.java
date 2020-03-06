package com.flink.demo.cases.case06;

import com.flink.demo.cases.common.datasource.UrlClickDataSource;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Timestamp;

/**
 * Created by DebugSy on 2019/7/10.
 *
 * java case 06
 * Flink SQL 训练 - 聚合函数训练
 * TUMBLE 滑动窗口
 * HOP 滚动窗口
 *
 */
public class FlinkSqlTraining_Agg {

    private static final Logger logger = LoggerFactory.getLogger(FlinkSqlTraining_Agg.class);

    private static String fields = "userId,username,url,clickTime.rowtime";

    private static String tumbleWindowSql = "select username, count(*) as cnt, " +
            "TUMBLE_START(rowtime, INTERVAL '10' SECOND) as window_start, " +
            "TUMBLE_END(rowtime, INTERVAL '10' SECOND) as window_end " +
            "from clicks " +
            "group by username, " +
            "TUMBLE(rowtime, INTERVAL '10' SECOND)";

    private static String hopWindowSql = "select username, count(*) as cnt, avg(userId) as userId_avg," +
            "HOP_ROWTIME(clickTime, INTERVAL '5' SECOND, INTERVAL '10' SECOND) as window_rowtime, " +
            "HOP_START(clickTime, INTERVAL '5' SECOND, INTERVAL '10' SECOND) as window_start, " +
            "HOP_END(clickTime, INTERVAL '5' SECOND, INTERVAL '10' SECOND) as window_end " +
            "from clicks " +
            "group by username, " +
            "HOP(clickTime, INTERVAL '5' SECOND, INTERVAL '10' SECOND)";

    private static String sessionWindowSql = "";

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.getConfig().setLatencyTrackingInterval(1000);

        StreamTableEnvironment tableEnv = TableEnvironment.getTableEnvironment(env);

        DataStreamSource<Tuple4<Integer, String, String, Timestamp>> sourceStream = env.addSource(new UrlClickDataSource());

        KeyedStream<Tuple4<Integer, String, String, Timestamp>, Tuple> keyedStream = sourceStream.assignTimestampsAndWatermarks(
                new AscendingTimestampExtractor<Tuple4<Integer, String, String, Timestamp>>() {
            @Override
            public long extractAscendingTimestamp(Tuple4<Integer, String, String, Timestamp> element) {
                return element.f3.getTime();
            }
        }).keyBy(0);

        tableEnv.registerDataStream("clicks", keyedStream, fields);

//        Table sqlQuery = tableEnv.sqlQuery(tumbleWindowSql);
        Table sqlQuery = tableEnv.sqlQuery(hopWindowSql);

        DataStream<Row> sinkStream = tableEnv.toAppendStream(sqlQuery, Row.class);
        sinkStream.printToErr();


        env.execute("Flink SQL Training");
    }

}
