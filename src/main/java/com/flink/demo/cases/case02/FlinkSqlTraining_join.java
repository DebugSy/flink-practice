package com.flink.demo.cases.case02;

import com.flink.demo.cases.common.datasource.UrlClickDataSource;
import com.flink.demo.cases.common.datasource.UserDataSource;
import org.apache.flink.api.java.tuple.Tuple;
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
 * java case 02
 * Flink SQL 训练 - 两个流基于时间窗口连接训练
 *
 */
public class FlinkSqlTraining_join {

    private static final Logger logger = LoggerFactory.getLogger(FlinkSqlTraining_join.class);

    /**
     * 两个流基于时间窗口的join
     */
    private static String innerJoinWithTimeWindowSql = "SELECT * FROM clicks c, users u " +
            "WHERE c.username = u.username " +
            "AND c.clickTime BETWEEN u.activityTime - INTERVAL '3' SECOND AND u.activityTime + INTERVAL '5' SECOND";


    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        StreamTableEnvironment tableEnv = TableEnvironment.getTableEnvironment(env);

        DataStreamSource<Tuple4<Integer, String, String, Timestamp>> clickStream = env.addSource(new UrlClickDataSource());

        //通过时间戳分配器/水印生成器指定时间戳和水印
        KeyedStream<Tuple4<Integer, String, String, Timestamp>, Tuple> keyedClickStream = clickStream
                .assignTimestampsAndWatermarks(new AscendingTimestampExtractor<Tuple4<Integer, String, String, Timestamp>>() {
            @Override
            public long extractAscendingTimestamp(Tuple4<Integer, String, String, Timestamp> element) {
                return element.f3.getTime();
            }
        }).keyBy(0);

        DataStreamSource<Tuple4<Integer, String, String, Timestamp>> userStream = env.addSource(new UserDataSource());
        //通过时间戳分配器/水印生成器指定时间戳和水印
        KeyedStream<Tuple4<Integer, String, String, Timestamp>, Tuple> keyedUserStream = userStream
                .assignTimestampsAndWatermarks(new AscendingTimestampExtractor<Tuple4<Integer, String, String, Timestamp>>() {
                    @Override
                    public long extractAscendingTimestamp(Tuple4<Integer, String, String, Timestamp> element) {
                        return element.f3.getTime();
                    }
                }).keyBy(0);

        tableEnv.registerDataStream("clicks", keyedClickStream, UrlClickDataSource.CLICK_FIELDS);
        tableEnv.registerDataStream("users", keyedUserStream, UserDataSource.USER_FIELDS);

        Table sqlQuery = tableEnv.sqlQuery(innerJoinWithTimeWindowSql);

        DataStream<Row> sinkStream = tableEnv.toAppendStream(sqlQuery, Row.class);
        sinkStream.printToErr();


        env.execute("Flink SQL Training");
    }

}
