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

public class FlinkSQLCEPTraining {

    public static String CEP_SQL = "select * \n" +
            "from cep_source \n" +
            "MATCH_RECOGNIZE ( \n" +
            "\tPARTITION BY USR_ID \n" + //同一个用户
            "\tORDER BY POST_TM \n" +
            "\tMEASURES \n" +
            "\t\tA.CDR_ID as A_CDR_ID, \n" +
            "\t\tA.USR_ID as A_USR_ID, \n" +
            "\t\tA.USER_BRAND as A_USER_BRAND, \n" +
            "\t\tA.RCV_AMT as A_RCV_AMT, \n" +
            "\t\tA.OFF_FLAG as A_OFF_FLAG, \n" +
            "\t\tA.POST_TM as A_POST_TM, \n" +
            "\t\tB.CDR_ID as B_CDR_ID, \n" +
            "\t\tB.USR_ID as B_USR_ID, \n" +
            "\t\tB.USER_BRAND as B_USER_BRAND, \n" +
            "\t\tB.RCV_AMT as B_RCV_AMT, \n" +
            "\t\tB.OFF_FLAG as B_OFF_FLAG, \n" +
            "\t\tB.POST_TM as B_POST_TM \n" +
            "\tONE ROW PER MATCH \n" +
            "\tAFTER MATCH SKIP PAST LAST ROW \n" +
            "\tPATTERN (A C* B) \n" +
            "\tDEFINE \n" +
            "\t\tA AS A.RCV_AMT > 98.0, \n" +
            "\t\tB AS B.RCV_AMT < 3.0, \n" +
            "\t\tC AS C.RCV_AMT >= 3.0 and C.RCV_AMT <= 98.0 \n" +
            ")";

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
                        return Long.valueOf(element.getField(6).toString());
                    }
                }).keyBy(0);

        tableEnv.registerDataStream("cep_source", keyedClickStream, CEPDataSource.CLICK_FIELDS_PROCTIME);

        Table sqlQuery = tableEnv.sqlQuery(CEP_SQL);

        DataStream<Row> sinkStream = tableEnv.toAppendStream(sqlQuery, Row.class);
        sinkStream.printToErr();


        env.execute("Flink SQL Training");

    }

}
