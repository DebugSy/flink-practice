package com.flink.demo.cases.case05;

import com.flink.demo.cases.common.datasource.UrlClickDataSource;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Timestamp;

import static com.flink.demo.cases.common.datasource.UrlClickDataSource.CLICK_FIELDS_WITH_ROWTIME;

/**
 * Created by DebugSy on 2019/8/6.
 */
public class FlinkSqlTraining_over {

    private static final Logger logger = LoggerFactory.getLogger(FlinkSqlTraining_over.class);

    private static String overSql = "select url as click_url,count(username)" +
            " over (partition by url order by clickActionTime rows between 4 preceding and current row)" +
            " from users";

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        DataStreamSource<Tuple4<Integer, String, String, Timestamp>> clickDataSource = env.addSource(new UrlClickDataSource());
        KeyedStream<Tuple4<Integer, String, String, Timestamp>, Tuple> keyedStream = clickDataSource.assignTimestampsAndWatermarks(
                new AscendingTimestampExtractor<Tuple4<Integer, String, String, Timestamp>>() {
            @Override
            public long extractAscendingTimestamp(Tuple4<Integer, String, String, Timestamp> element) {
                return element.f3.getTime();
            }
        }).keyBy(0);

        StreamTableEnvironment tEnv = TableEnvironment.getTableEnvironment(env);
        tEnv.registerDataStream("users", keyedStream, CLICK_FIELDS_WITH_ROWTIME);

        Table table = tEnv.sqlQuery(overSql);
        DataStream<Row> sinkDataStream = tEnv.toAppendStream(table, Row.class);
        sinkDataStream.addSink(new SinkFunction<Row>() {
            @Override
            public void invoke(Row value, Context context) throws Exception {
                logger.info("Print {}", value);
            }
        });


        env.execute("Flink sql traning over");
    }

}
