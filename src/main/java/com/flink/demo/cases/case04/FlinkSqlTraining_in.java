package com.flink.demo.cases.case04;

import com.flink.demo.cases.common.datasource.OutOfOrderDataSource;
import com.flink.demo.cases.common.datasource.UserDataSource;
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
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Timestamp;

/**
 * Created by DebugSy on 2019/7/30.
 *
 * java case 04
 * Flink SQL 训练 - in关键字测试
 * 如果不加时间窗口，需要tableSink支持retract，即toRetractStream()
 */
public class FlinkSqlTraining_in {

    private static final Logger logger = LoggerFactory.getLogger(FlinkSqlTraining_in.class);

    private static String userFields = "userId,username,address,activityTime";

    private static String clickFields = "username,url,clickTime";

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        DataStreamSource<Tuple4<Integer, String, String, Timestamp>> userSource = env.addSource(new UserDataSource());

        DataStreamSource<Tuple3<String, String, Timestamp>> clickStream = env.addSource(new OutOfOrderDataSource());
        KeyedStream<Tuple3<String, String, Timestamp>, Tuple> keyedStream = clickStream.assignTimestampsAndWatermarks(
                new BoundedOutOfOrdernessTimestampExtractor<Tuple3<String, String, Timestamp>>(Time.seconds(10)) {
                    @Override
                    public long extractTimestamp(Tuple3<String, String, Timestamp> element) {
                        return element.f2.getTime();
                    }
                }
                ).keyBy(0);

        StreamTableEnvironment tEnv = TableEnvironment.getTableEnvironment(env);
        tEnv.registerDataStream("users", userSource, userFields);
        tEnv.registerDataStream("clicks", keyedStream, clickFields);

        Table table = tEnv.sqlQuery("select `users`.`userId`,username,address,activityTime from users where username in (select username from clicks)");

//        DataStream<Row> sinkStream = tEnv.toAppendStream(table, Row.class);
//        sinkStream.addSink(new SinkFunction<Row>() {
//            @Override
//            public void invoke(Row value, Context context) throws Exception {
//                logger.info("Print {}", value);
//            }
//        });

        //不加时间窗口需要支持retract
        DataStream<Tuple2<Boolean, Row>> retractSinkStream = tEnv.toRetractStream(table, Row.class);
        retractSinkStream.addSink(new SinkFunction<Tuple2<Boolean, Row>>() {
            @Override
            public void invoke(Tuple2<Boolean, Row> value, Context context) throws Exception {
                logger.info("Print {}", value);
            }
        });

        env.execute("Flink sql training in");
    }

}
