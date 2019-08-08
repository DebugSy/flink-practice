package com.flink.demo.cases.case02;

import com.flink.demo.cases.common.datasource.UrlClickDataSource;
import com.flink.demo.cases.common.functions.udtf.UserTableFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Timestamp;

/**
 * Created by DebugSy on 2019/8/8.
 */
public class FlinkTableTraining_join_function {

    private static final Logger logger = LoggerFactory.getLogger(FlinkTableTraining_join_function.class);


    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        DataStreamSource<Tuple3<String, String, Timestamp>> clickSource = env.addSource(new UrlClickDataSource());
        KeyedStream<Tuple3<String, String, Timestamp>, Tuple> keyedStream = clickSource.assignTimestampsAndWatermarks(new AscendingTimestampExtractor<Tuple3<String, String, Timestamp>>() {
            @Override
            public long extractAscendingTimestamp(Tuple3<String, String, Timestamp> element) {
                return element.f2.getTime();
            }
        }).keyBy(0);

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        /* 方式一：将datastream注册为表 */
//        tableEnv.registerDataStream("clicks", keyedStream, UrlClickDataSource.CLICK_FIELDS);

        /* 方式二：将datastream转换为表 */
        Table urlClickTable = tableEnv.fromDataStream(keyedStream, UrlClickDataSource.CLICK_FIELDS);

        tableEnv.registerFunction("users", new UserTableFunction());

        Table result = urlClickTable.joinLateral("users(username) as (name,address,telephone)");

        DataStream<Row> sinkStream = tableEnv.toAppendStream(result, Row.class);
        sinkStream.printToErr();

//        String explain = tableEnv.explain(result);
//        System.err.println(explain);

        env.execute("Flink Table Training join lateral table");
    }

}
