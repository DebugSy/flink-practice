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

    public static String MAP_SQL = "select " +
            "CDR_ID,USR_ID,USER_BRAND,ORD_STS,RCV_AMT,OFF_FLAG,POST_TM,POST_TM / 60000 * 60000 as truncatedTime " +
            "from cep_source";

    public static String CEP_SQL = "select \n" +
            "rtc_current_time(CAST(TUMBLE_PROCTIME(procTime, INTERVAL '2' SECOND) as varchar)) as t1,\n" +
            "truncatedTime as t2,\n" +
            "rtc_current_time(CAST(TUMBLE_PROCTIME(procTime, INTERVAL '2' SECOND) as varchar)) - truncatedTime as proc_time \n" +
            "from mapResultTable \n" +
            "group by TUMBLE(procTime, INTERVAL '2' SECOND), \n" +
            "truncatedTime \n";

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        DataStream<Row> cepSource = env
                .addSource(new CEPDataSource())
                .returns(CEPDataSource.CLICK_TYPEINFO);

        tableEnv.registerDataStream("cep_source", cepSource, CEPDataSource.CLICK_FIELDS);
        tableEnv.registerFunction("rtc_current_time", new TimeUDF());

        Table mapResult = tableEnv.sqlQuery(MAP_SQL);
        DataStream<Row> rowDataStream = tableEnv.toAppendStream(mapResult, mapResult.getSchema().toRowType());
        tableEnv.registerDataStream("mapResultTable", rowDataStream,  CEPDataSource.CLICK_FIELDS_truncatedTime);

        Table sqlQuery = tableEnv.sqlQuery(CEP_SQL);

        DataStream<Row> sinkStream = tableEnv.toAppendStream(sqlQuery, Row.class);
        sinkStream.printToErr();


        env.execute("Flink SQL Training");

    }

}
