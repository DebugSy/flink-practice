package com.flink.demo.cases.case13;

import com.flink.demo.cases.common.datasource.UrlClickRowDataSource;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
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
public class RetractSinkTraining {

    private static final Logger logger = LoggerFactory.getLogger(RetractSinkTraining.class);

    private static String fields = "username,url,clickTime,rowtime.rowtime";

    private static String hopWindowSql = "insert into test_sink select username, count(*) as cnt " +
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

    private static TypeInformation returnTypeInfo = Types.ROW(
            new String[]{"username", "cnt"},
            new TypeInformation[]{
                    Types.STRING(),
                    Types.LONG()
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

        tableEnv.registerTableSink("test_sink", new RetractTableSink((RowTypeInfo) returnTypeInfo));

        tableEnv.sqlUpdate(hopWindowSql);

        env.execute("Flink SQL Training");
    }

}
