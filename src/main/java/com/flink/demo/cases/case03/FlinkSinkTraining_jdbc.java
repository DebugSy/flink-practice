package com.flink.demo.cases.case03;

import com.flink.demo.cases.common.datasink.AppendSink_jdbc;
import com.flink.demo.cases.common.datasource.UserDataSource;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Timestamp;

/**
 * Created by DebugSy on 2019/7/29.
 */
public class FlinkSinkTraining_jdbc {

    private static final Logger logger = LoggerFactory.getLogger(FlinkSinkTraining_jdbc.class);

    private static String userFields = "userId,username,address,activityTime";

    /**
     * 1. 字段顺序应该与字段类型一致
     * 2. sink表字段顺序应该与select语句的字段顺序一致
     */
    private static String[] fieldNames = {"userId_alis", "username_alis", "address_alis"};

    private static TypeInformation[] typeInfos = {
            TypeInformation.of(Integer.class),
            TypeInformation.of(String.class),
            TypeInformation.of(String.class)
    };

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(1);

        StreamTableEnvironment tableEnv = TableEnvironment.getTableEnvironment(env);

        DataStreamSource<Tuple4<Integer, String, String, Timestamp>> userStream = env.addSource(new UserDataSource());
        tableEnv.registerDataStream("users", userStream, userFields);
//        tableEnv.registerTableSink("jdb1c_sink", new AppendSink_jdbc(fieldNames, typeInfos));
        tableEnv.registerTableSink("jdb1c_sink", fieldNames, typeInfos, new AppendSink_jdbc());

//        tableEnv.sqlUpdate("insert into jdb1c_sink SELECT username,count(username) FROM users GROUP BY username");
        tableEnv.sqlUpdate("insert into jdb1c_sink SELECT userId as userId_alis,username as username_alis,address as address_alis FROM users");

//        DataStream<Row> sinkStream = tableEnv.toAppendStream(table, Row.class);
//        sinkStream.addSink(new SinkFunction<Row>() {
//            @Override
//            public void invoke(Row value, Context context) throws Exception {
//                logger.info("print {}", value);
//            }
//        });

        env.execute("Flink sql sink training for jdbc");
    }

}
