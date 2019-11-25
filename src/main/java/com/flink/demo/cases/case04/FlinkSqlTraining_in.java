package com.flink.demo.cases.case04;

import com.flink.demo.cases.case06.MyUDAF;
import com.flink.demo.cases.common.datasource.UserDataSource;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;
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
 * Created by DebugSy on 2019/7/30.
 *
 * java case 04
 * Flink SQL 训练 - in关键字测试
 * 如果不加时间窗口，需要tableSink支持retract，即toRetractStream()
 */
public class FlinkSqlTraining_in {

    private static final Logger logger = LoggerFactory.getLogger(FlinkSqlTraining_in.class);

    private static String userFields = "userId,username,address,activityTime.rowtime";

    private static String clickFields = "userId,username,url,clickTime";

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        DataStreamSource<Tuple4<Integer, String, String, Timestamp>> userSource1 = env.addSource(new UserDataSource());
        KeyedStream<Tuple4<Integer, String, String, Timestamp>, Tuple> keyedStream1 = userSource1.assignTimestampsAndWatermarks(
                new AscendingTimestampExtractor<Tuple4<Integer, String, String, Timestamp>>() {
                    @Override
                    public long extractAscendingTimestamp(Tuple4<Integer, String, String, Timestamp> element) {
                        return element.f3.getTime();
                    }
                }
        ).keyBy(0);

        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        tEnv.registerDataStream("users1", keyedStream1, userFields);
        tEnv.registerDataStream("users2", keyedStream1, userFields);
        tEnv.registerFunction("my_agg", new MyUDAF());

        Table table = tEnv.sqlQuery("SELECT username,my_agg(u1.address) as address FROM users1 u1  \n" +
                "group by TUMBLE(u1.activityTime, INTERVAL '5' SECOND),u1.username \n");

//        tEnv.registerTable("addresses", table);
//        tEnv.registerFunction("addrs", new AddressUDTF());
//
//        Table table1 = tEnv.sqlQuery("select * from users1 u1, LATERAL TABLE(addrs(u1.username)) as t2(address)\n" +
//                "where POSITION(u1.address in t2.address) = 0 \n");

        //不加时间窗口需要支持retract
        DataStream<Tuple2<Boolean, Row>> retractSinkStream = tEnv.toRetractStream(table, Row.class);
        retractSinkStream.printToErr();

        env.execute("Flink sql training in");
    }

}
