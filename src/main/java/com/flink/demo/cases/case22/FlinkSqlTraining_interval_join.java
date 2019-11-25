package com.flink.demo.cases.case22;

import com.flink.demo.cases.common.datasource.UserDataSource;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Timestamp;

/**
 * Created by DebugSy on 2019/7/30.
 *
 * java case 22
 * Flink SQL 训练 - Time Interval Join
 */
public class FlinkSqlTraining_interval_join {

    private static final Logger logger = LoggerFactory.getLogger(FlinkSqlTraining_interval_join.class);

    private static String userFields = "userId,username,address,activityTime";

    private static String leftOuterJoinSql = "SELECT u1.username,u1.activityTime FROM users2 u2 " +
            "LEFT OUTER JOIN users1 u1 ON u1.username = u2.username " +
            "WHERE u2.activityTime BETWEEN u1.activityTime - INTERVAL '4' SECOND AND u1.activityTime - INTERVAL '2' SECOND ";

    private static String innerJoinSql = "SELECT u1.username,u1.address,u2.address,u1.activityTime " +
            "FROM users2 u2,users1 u1 WHERE u1.username = u2.username " +
            "AND u2.activityTime BETWEEN u1.activityTime - INTERVAL '4' SECOND AND u1.activityTime - INTERVAL '2' SECOND ";

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        DataStreamSource<Tuple4<Integer, String, String, Timestamp>> userSource = env.addSource(new UserDataSource());

        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        tEnv.registerDataStream("users1", userSource, userFields);
        tEnv.registerDataStream("users2", userSource, userFields);

        Table table = tEnv.sqlQuery(innerJoinSql);


        DataStream<Row> rowDataStream = tEnv.toAppendStream(table, Row.class);
        rowDataStream.addSink(new SinkFunction<Row>() {
            @Override
            public void invoke(Row value) throws Exception {
                logger.info("Print => {}", value);
            }
        });

        env.execute("Flink sql training in");
    }

}
