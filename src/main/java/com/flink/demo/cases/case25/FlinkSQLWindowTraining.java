package com.flink.demo.cases.case25;

import com.flink.demo.cases.common.datasource.UrlClickRowDataSource;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.AllWindowedStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.WindowAssigner;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.api.windowing.windows.Window;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import java.util.Collection;

/**
 * Created by P0007 on 2020/3/11.
 */
public class FlinkSQLWindowTraining {

    private static String tumbleWindowSql = "select username, count(*) as cnt, " +
            "TUMBLE_START(rowtime, INTERVAL '10' SECOND) as window_start, " +
            "TUMBLE_END(rowtime, INTERVAL '10' SECOND) as window_end " +
            "from clicks " +
            "group by username, " +
            "TUMBLE(rowtime, INTERVAL '10' SECOND)";

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(4);


        SingleOutputStreamOperator<Row> clickStream = env.addSource(new UrlClickRowDataSource())
                .returns(UrlClickRowDataSource.USER_CLICK_TYPEINFO)
                .name("Url Click Stream");

        KeyedStream keyedStream = clickStream.keyBy(0);

        AllWindowedStream<Row, TimeWindow> windowedStream = clickStream.timeWindowAll(Time.seconds(10), Time.seconds(5));
        SingleOutputStreamOperator<Row> aggregate = windowedStream.aggregate(new AggregateFunction<Row, Long, Row>() {
            @Override
            public Long createAccumulator() {
                return 0L;
            }

            @Override
            public Long add(Row value, Long accumulator) {
                return ++accumulator;
            }

            @Override
            public Row getResult(Long accumulator) {
                Row row = new Row(1);
                row.setField(0, accumulator);
                return row;
            }

            @Override
            public Long merge(Long a, Long b) {
                return a + b;
            }
        });

        aggregate.printToErr();


        env.execute("Flink Window SQL Training");
    }

}
