package com.flink.demo.cases.case02;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Random;

/**
 * Created by P0007 on 2019/8/30.
 */
public class FlinkTableTraining_join {

    private static final Logger logger = LoggerFactory.getLogger(FlinkSqlTraining_join.class);

    private static String clickFields = "userId,orderKey,address";

    private static String userFields = "userId,orderKey,address";

    /**
     * 看样子只支持第一个条件是等值的join
     *
     * see: https://ci.apache.org/projects/flink/flink-docs-master/dev/table/sql.html#joins
     */
    private static String innerJoinWithTimeWindowSql = "SELECT * FROM clicks c INNER JOIN users u " +
            "ON c.userId = u.userId and c.orderKey >= u.orderKey";


    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        ArrayList<Tuple3<Integer, Integer, String>> leftCollection = new ArrayList<>(20);
        Random random = new Random(System.currentTimeMillis());
        long time = 1567149973696L;
        for (int i = 20; i < 20 + 20; i++) {
            int userId = 65 + random.nextInt(5);
            time = time + 10000L;
            leftCollection.add(new Tuple3<>(userId, i, "left_address" + i));
        }
        DataStreamSource<Tuple3<Integer, Integer, String>> left = env.fromCollection(leftCollection);

        ArrayList<Tuple3<Integer, Integer, String>> rightCollection = new ArrayList<>(20);
        for (int i = 15; i < 20 + 15; i++) {
            int userId = 65 + random.nextInt(5);
            rightCollection.add(new Tuple3<>(userId, i, "right_address" + i));
        }
        DataStreamSource<Tuple3<Integer, Integer, String>> right = env.fromCollection(rightCollection);

        tableEnv.registerDataStream("clicks", left, clickFields);
        tableEnv.registerDataStream("users", right, userFields);

        Table table = tableEnv.sqlQuery(innerJoinWithTimeWindowSql);
        DataStream<Tuple2<Boolean, Row>> toRetractStream = tableEnv.toRetractStream(table, Row.class);
        toRetractStream.printToErr();

        env.execute("Flink SQL Training");
    }
}
