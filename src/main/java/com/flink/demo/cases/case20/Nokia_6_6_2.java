package com.flink.demo.cases.case20;

import com.flink.demo.cases.common.datasource.CEPDataSource;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import java.sql.Timestamp;

public class Nokia_6_6_2 {

    public static String CEP_SQL = "select " +
            "rtc_current_time(CAST(TUMBLE_ROWTIME(POST_TM, INTERVAL '2' SECOND) as varchar)) as t1," +
            "rtc_current_time(TUMBLE_ROWTIME(POST_TM, INTERVAL '2' SECOND)) as t2," +
            "rtc_current_time(CAST(TUMBLE_ROWTIME(POST_TM, INTERVAL '2' SECOND) as varchar)) - rtc_current_time(TUMBLE_ROWTIME(POST_TM, INTERVAL '2' SECOND)) as proc_time " +
            "from cep_source " +
            "group by TUMBLE(POST_TM, INTERVAL '2' SECOND)";

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        DataStream<Row> cepSource = env
                .addSource(new CEPDataSource())
                .returns(CEPDataSource.CLICK_TYPEINFO);

        //通过时间戳分配器/水印生成器指定时间戳和水印
        KeyedStream<Row, Tuple> keyedClickStream = cepSource
                .assignTimestampsAndWatermarks(new AscendingTimestampExtractor<Row>() {
                    @Override
                    public long extractAscendingTimestamp(Row element) {
                        return Timestamp.valueOf(element.getField(6).toString()).getTime();
                    }
                }).keyBy(0);

        tableEnv.registerDataStream("cep_source", keyedClickStream, CEPDataSource.CLICK_FIELDS);
        tableEnv.registerFunction("rtc_current_time", new TimeUDF());

        Table sqlQuery = tableEnv.sqlQuery(CEP_SQL);

        DataStream<Row> sinkStream = tableEnv.toAppendStream(sqlQuery, Row.class);
        sinkStream.printToErr();


        env.execute("Flink SQL Training");

    }

}
