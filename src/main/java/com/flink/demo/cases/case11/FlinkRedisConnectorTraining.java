package com.flink.demo.cases.case11;

import com.flink.demo.cases.common.datasource.UrlClickDataSource;
import com.flink.demo.cases.common.datasource.UrlClickRowDataSource;
import org.apache.flink.api.common.typeinfo.TypeInformation;
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
import org.apache.flink.streaming.connectors.redis.RedisSink;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.Types;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Timestamp;

/**
 * Created by P0007 on 2019/9/6.
 */
public class FlinkRedisConnectorTraining {

    private static final Logger logger = LoggerFactory.getLogger(FlinkRedisConnectorTraining.class);

    private static String fields = "username,url,clickTime,rowtime.rowtime";

    private static String hopWindowSql = "select username, count(*) as cnt " +
            "from clicks " +
            "group by username";

    private static TypeInformation userClickTypeInfo = Types.ROW(
            new String[]{"userId", "username", "url", "clickTime"},
            new TypeInformation[]{
                    Types.INT(),
                    Types.STRING(),
                    Types.STRING(),
                    Types.SQL_TIMESTAMP()
            });

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.getConfig().setLatencyTrackingInterval(1000);

        StreamTableEnvironment tableEnv = TableEnvironment.getTableEnvironment(env);
        DataStream<Row> sourceStream = env
                .addSource(new UrlClickRowDataSource())
                .returns(userClickTypeInfo);;
        KeyedStream<Row, Tuple> keyedStream = sourceStream.assignTimestampsAndWatermarks(new AscendingTimestampExtractor<Row>() {
            @Override
            public long extractAscendingTimestamp(Row row) {
                return Timestamp.valueOf(row.getField(3).toString()).getTime();
            }
        }).keyBy(0);

        tableEnv.registerDataStream("clicks", keyedStream, fields);

//        Table sqlQuery = tableEnv.sqlQuery(tumbleWindowSql);
        Table sqlQuery = tableEnv.sqlQuery(hopWindowSql);

        DataStream<Tuple2<Boolean, Row>> sinkStream = tableEnv.toRetractStream(sqlQuery, Row.class);
//        DataStream<Row> sinkStream = tableEnv.toAppendStream(sqlQuery, Row.class);

        FlinkJedisPoolConfig conf = new FlinkJedisPoolConfig.Builder().setHost("192.168.1.83").setPort(6378).build();
        sinkStream.addSink(new RedisSink<Tuple2<Boolean, Row>>(conf, new RedisExampleMapper(0, "flink-sink-2")));

        env.execute("Flink SQL Training");
    }

}
