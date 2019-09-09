package com.flink.demo.cases.case06;

import com.flink.demo.cases.common.datasource.UrlClickDataSource;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
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

    private static String fields = "username,url,clickTime,rowtime.rowtime";

    private static String tumbleWindowSql = "select username, count(*) as cnt, " +
            "TUMBLE_START(rowtime, INTERVAL '10' SECOND) as window_start, " +
            "TUMBLE_END(rowtime, INTERVAL '10' SECOND) as window_end " +
            "from clicks " +
            "group by username, " +
            "TUMBLE(rowtime, INTERVAL '10' SECOND)";

    private static String hopWindowSql = "select username, count(*) as cnt, " +
            "HOP_ROWTIME(rowtime, INTERVAL '5' SECOND, INTERVAL '10' SECOND) as window_rowtime, " +
            "HOP_START(rowtime, INTERVAL '5' SECOND, INTERVAL '10' SECOND) as window_start, " +
            "HOP_END(rowtime, INTERVAL '5' SECOND, INTERVAL '10' SECOND) as window_end " +
            "from clicks " +
            "where url like '%/api/H%'" +
            "group by username, " +
            "HOP(rowtime, INTERVAL '5' SECOND, INTERVAL '10' SECOND)";

    private static String sessionWindowSql = "";

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.getConfig().setLatencyTrackingInterval(1000);

        StreamTableEnvironment tableEnv = TableEnvironment.getTableEnvironment(env);

        DataStreamSource<Tuple3<String, String, Timestamp>> sourceStream = env.addSource(new UrlClickDataSource());

        KeyedStream<Tuple3<String, String, Timestamp>, Tuple> keyedStream = sourceStream.assignTimestampsAndWatermarks(new AscendingTimestampExtractor<Tuple3<String, String, Timestamp>>() {
            @Override
            public long extractAscendingTimestamp(Tuple3<String, String, Timestamp> element) {
                return element.f2.getTime();
            }
        }).keyBy(0);

        tableEnv.registerDataStream("clicks", keyedStream, fields);

//        Table sqlQuery = tableEnv.sqlQuery(tumbleWindowSql);
        Table sqlQuery = tableEnv.sqlQuery(hopWindowSql);

        DataStream<Tuple2<Boolean, Row>> sinkStream = tableEnv.toRetractStream(sqlQuery, Row.class);
        sinkStream.addSink(new SinkFunction<Tuple2<Boolean, Row>>() {
            @Override
            public void invoke(Tuple2<Boolean, Row> value, Context context) throws Exception {
                logger.error("print retract:{} -> {}", value.f0, value.f1);
            }
        }).name("Print to Std.Error");


        env.execute("Flink SQL Training");
    }

}
