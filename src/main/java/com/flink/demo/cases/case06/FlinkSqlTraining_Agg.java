package com.flink.demo.cases.case06;

import com.flink.demo.cases.common.datasource.UrlClickDataSource;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
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
        Configuration configuration = new Configuration();
        configuration.setString("state.checkpoints.num-retained", "10");
        StreamExecutionEnvironment env = StreamExecutionEnvironment
                .createLocalEnvironment(2, configuration);
        env.setParallelism(3);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.enableCheckpointing(5000);
        CheckpointConfig checkpointConfig = env.getCheckpointConfig();
        checkpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        checkpointConfig.setMinPauseBetweenCheckpoints(500);
        checkpointConfig.setCheckpointTimeout(1000 * 60);
        checkpointConfig.setMaxConcurrentCheckpoints(10);
        checkpointConfig.enableExternalizedCheckpoints(
                CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        env.setStateBackend((StateBackend)new RocksDBStateBackend("file:///tmp/flink-checkpoints/"));

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

        Table sqlQuery = tableEnv.sqlQuery(tumbleWindowSql);

        DataStream<Tuple2<Boolean, Row>> sinkStream = tableEnv.toRetractStream(sqlQuery, Row.class);
        sinkStream.printToErr();


        env.execute("Flink SQL Training");
    }

}
