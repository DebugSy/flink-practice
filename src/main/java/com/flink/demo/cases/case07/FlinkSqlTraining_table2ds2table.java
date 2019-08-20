package com.flink.demo.cases.case07;

import com.flink.demo.cases.common.datasource.OutOfOrderDataSource;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Timestamp;

/**
 * Created by P0007 on 2019/8/19.
 *
 * 目的：有的场景需要在第一次group by以后，将时间列选择出来作为下一次grouop by的时间列
 * 如：
 * <1>第一次聚合</1>
 * insert into sink
 *  select
 *      username,
 *      cast(TUMBLE_END(rowtime, INTERVAL '10' SECOND) as TIMESTAMP) as window_end
 * group by
 *      TUMBLE(rowtime, INTERVAL '10' SECOND),
 *      username;
 *
 * <2>第二次聚合</2>
 * select username from sink group by TUMBLE(window_end, INTERVAL '10' SECOND), username
 */
public class FlinkSqlTraining_table2ds2table {

    private static final Logger logger = LoggerFactory.getLogger(FlinkSqlTraining_table2ds2table.class);

    private static String fields = "username,url,clickTime,rowtime.rowtime";

    private static String tumbleWindowSql = "select username, count(*) as cnt, " +
            "cast(TUMBLE_START(rowtime, INTERVAL '10' SECOND)  as TIMESTAMP)as window_start, " +
            "cast(TUMBLE_END(rowtime, INTERVAL '10' SECOND) as TIMESTAMP) as window_end " +
            "from clicks " +
            "group by username, " +
            "TUMBLE(rowtime, INTERVAL '10' SECOND)";
    
    private static String secondSql = "select username from ds group by TUMBLE(window_end, INTERVAL '10' SECOND), username";


    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        StreamTableEnvironment tableEnv = TableEnvironment.getTableEnvironment(env);

        DataStreamSource<Tuple3<String, String, Timestamp>> sourceStream = env.addSource(new OutOfOrderDataSource());

        KeyedStream<Tuple3<String, String, Timestamp>, Tuple> keyedStream = sourceStream.assignTimestampsAndWatermarks(new AscendingTimestampExtractor<Tuple3<String, String, Timestamp>>() {
            @Override
            public long extractAscendingTimestamp(Tuple3<String, String, Timestamp> element) {
                return element.f2.getTime();
            }
        }).keyBy(0);

        tableEnv.registerDataStream("clicks", keyedStream, fields);

        Table sqlQuery = tableEnv.sqlQuery(tumbleWindowSql);

        TableSchema schema = sqlQuery.getSchema();
        String[] fieldNames = schema.getFieldNames();
        /**
         * 设置下一个group by的时间列
         */
        int timeColIndex = ArrayUtils.indexOf(fieldNames, "window_end");
        fieldNames[timeColIndex] = fieldNames[timeColIndex].concat(".rowtime");
        String joinString = StringUtils.join(fieldNames, ",");
        TypeInformation<Row> rowTypeInformation = schema.toRowType();

        DataStream<Row> rowDataStream = tableEnv.toAppendStream(sqlQuery, rowTypeInformation);
        tableEnv.registerDataStream("ds", rowDataStream, joinString);

        Table table = tableEnv.sqlQuery(secondSql);

        DataStream<Row> sinkStream = tableEnv.toAppendStream(table, Row.class);
        sinkStream.printToErr();


        env.execute("Flink SQL Training");
    }

}
