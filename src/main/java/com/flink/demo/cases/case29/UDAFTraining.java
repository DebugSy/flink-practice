package com.flink.demo.cases.case29;

import com.flink.demo.cases.case02.ConsoleTableSink;
import com.flink.demo.cases.common.datasource.UrlClickRowDataSource;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.Types;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import java.sql.Timestamp;

/**
 * Created by P0007 on 2020/10/30.
 *
 * UDAF实现flink 1.10的rownumber
 */
public class UDAFTraining {

    /**
     * 两个流基于时间窗口的join
     */
//    private static String udafSql1 = "select sum_udaf(userId) from clicks group by TUMBLE(clickTime, INTERVAL '5' SECOND)";
    private static String udafSql2 = "select row_num_udaf(Row(userId, username)) from clicks group by TUMBLE(clickTime, INTERVAL '5' SECOND)";

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        SingleOutputStreamOperator<Row> clickStream = env.addSource(new UrlClickRowDataSource())
                .returns(UrlClickRowDataSource.USER_CLICK_TYPEINFO);
        SingleOutputStreamOperator<Row> clickStreamAndWatermarks = clickStream.assignTimestampsAndWatermarks(new AscendingTimestampExtractor<Row>() {
            @Override
            public long extractAscendingTimestamp(Row element) {
                return Timestamp.valueOf(element.getField(3).toString()).getTime();
            }
        });

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        tableEnv.registerDataStream("clicks", clickStreamAndWatermarks, UrlClickRowDataSource.CLICK_FIELDS_WITH_ROWTIME);
        tableEnv.registerFunction("row_num_udaf", new RownumberUDAF());
        tableEnv.registerFunction("sum_udaf", new SumUDAF());

        Table table = tableEnv.sqlQuery(udafSql2);
        DataStream<Row> sinkStream = tableEnv.toAppendStream(table, Row.class);
        sinkStream.printToErr();

        env.execute("Flink Stream Join Training");
    }

}
