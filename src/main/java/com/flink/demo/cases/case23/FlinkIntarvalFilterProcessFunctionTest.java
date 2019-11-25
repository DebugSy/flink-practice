package com.flink.demo.cases.case23;

import com.flink.demo.cases.common.datasource.OutOfOrderDataSource;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;

import java.sql.Timestamp;

/**
 * Created by P0007 on 2019/10/28.
 */
public class FlinkIntarvalFilterProcessFunctionTest {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        DataStreamSource<Tuple4<Integer, String, String, Timestamp>> streamSource = env.addSource(new OutOfOrderDataSource());
        SingleOutputStreamOperator<Tuple4<Integer, String, String, Timestamp>> tuple4putStream = streamSource
                .assignTimestampsAndWatermarks(new AscendingTimestampExtractor<Tuple4<Integer, String, String, Timestamp>>() {
                    @Override
                    public long extractAscendingTimestamp(Tuple4<Integer, String, String, Timestamp> element) {
                        return element.f3.getTime();
                    }
                });

        KeyedStream<Tuple4<Integer, String, String, Timestamp>, Tuple> keyedStream = tuple4putStream.keyBy(1);
        SingleOutputStreamOperator<Tuple4<Integer, String, String, Timestamp>> sink = keyedStream.process(new FlinkIntarvalFilterProcessFunction(
                Time.seconds(-5).toMilliseconds(),
                Time.seconds(0).toMilliseconds(),
                false,
                false
        ));

        sink.printToErr();

        env.execute("Flink Interval Filter Training");

    }

}
