package com.flink.demo.cases.case12;

import com.flink.demo.cases.common.datasource.UrlClickRowDataSource;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import java.sql.Timestamp;

public class SQLBuiltInFunctionTraining {


    private static String sql =
            "SELECT TUMBLE_START(rowtime, INTERVAL '5' SECOND) as window_start," +
                    "TUMBLE_END(rowtime, INTERVAL '5' SECOND) as window_end," +
                    "userId,count(username) as cnt from clicks group by userId,tumble(rowtime, INTERVAL '5' SECOND)";

    private static String sql2 = "SELECT userId,username,SPLIT_INDEX(username, '=' , 0) as c1,SPLIT_INDEX(username, '=' , 1) as c2  from clicks";

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);

        SingleOutputStreamOperator<Row> clickStream = env.addSource(new UrlClickRowDataSource())
                .returns(UrlClickRowDataSource.USER_CLICK_TYPEINFO);
        SingleOutputStreamOperator<Row> clickStreamAndWatermarks = clickStream
                .assignTimestampsAndWatermarks(new AscendingTimestampExtractor<Row>() {
                    @Override
                    public long extractAscendingTimestamp(Row element) {
                        return Timestamp.valueOf(element.getField(3).toString()).getTime();
                    }
                });
        EnvironmentSettings settings = EnvironmentSettings.newInstance()
                .useBlinkPlanner()
                .inStreamingMode()
                .build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);
        tableEnv.createTemporaryView("clicks", clickStreamAndWatermarks, UrlClickRowDataSource.CLICK_FIELDS_WITH_ROWTIME);

        Table table = tableEnv.sqlQuery(sql2);
        DataStream<Row> rowDataStream = tableEnv.toAppendStream(table, Row.class);
        rowDataStream.printToErr();

        env.execute("Flink Stream Join Training");
    }

}
