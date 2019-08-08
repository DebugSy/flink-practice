package com.flink.demo.cases.case02;

import com.flink.demo.cases.common.datasource.UrlClickDataSource;
import com.flink.demo.cases.common.functions.udtf.UserTableFunction;
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
 * java case 02
 * Flink SQL 训练 - 连接函数训练
 * 与静态表join
 */
public class FlinkSqlTraining_join_function {

    private static final Logger logger = LoggerFactory.getLogger(FlinkSqlTraining_join_function.class);

    private static String fields = "username,url,clickTime,rowtime.rowtime";

    /**
     * 流连接侧表，类似lookup
     */
    private static String innerJoinSql = "select username,sid,cid from clicks, LATERAL TABLE(users(username)) as T(name,sid,cid)";

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        StreamTableEnvironment tableEnv = TableEnvironment.getTableEnvironment(env);

        DataStreamSource<Tuple3<String, String, Timestamp>> sourceStream = env.addSource(new UrlClickDataSource());

        KeyedStream<Tuple3<String, String, Timestamp>, Tuple> keyedStream = sourceStream
                .assignTimestampsAndWatermarks(new AscendingTimestampExtractor<Tuple3<String, String, Timestamp>>() {
            @Override
            public long extractAscendingTimestamp(Tuple3<String, String, Timestamp> element) {
                return element.f2.getTime();
            }
        }).keyBy(0);

        tableEnv.registerDataStream("clicks", keyedStream, fields);

        UserTableFunction lookupUDTF = new UserTableFunction();
        tableEnv.registerFunction("users", lookupUDTF);

        Table sqlQuery = tableEnv.sqlQuery(innerJoinSql);

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
