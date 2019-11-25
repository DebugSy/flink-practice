package com.flink.demo.cases.case23;

import com.flink.demo.cases.common.datasource.OutOfOrderDataSource;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.util.List;

/**
 * Created by P0007 on 2019/10/22.
 * <p>
 * java case 23 自定义Operator实现interval filter
 * 需求：检测用户当前地址是否包含在之前2小时去过的地址中
 * 参考flink interval join实现
 */
public class FlinkIntervalFilterOperatorTest {

    private static TypeInformation<Tuple4<Integer, String, String, Timestamp>> valueInfo =
            TypeInformation.of(new TypeHint<Tuple4<Integer, String, String, Timestamp>>() {
            });

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

        IntervalFilterOperator<Tuple> operator =
                new IntervalFilterOperator<Tuple>(
                        Time.seconds(-1).toMilliseconds(),
                        Time.seconds(0).toMilliseconds(),
                        false,
                        false,
                        new ProcessFunction<Tuple4<Integer, String, String, Timestamp>, Tuple4<Integer, String, String, Timestamp>>() {
                            @Override
                            public void processElement(Tuple4<Integer, String, String, Timestamp> value,
                                                       Context ctx, Collector<Tuple4<Integer, String, String, Timestamp>> out) throws Exception {
                                out.collect(value);
                            }
                        }
                );

        SingleOutputStreamOperator sink = keyedStream.transform(
                "interval filter",
                valueInfo,
                operator);

        sink.printToErr();

        env.execute("Flink State Training");
    }

}
