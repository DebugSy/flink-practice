package com.flink.demo.cases.case06;

import com.flink.demo.cases.common.datasource.OutOfOrderDataSource;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Timestamp;

/**
 * Created by DebugSy on 2019/7/17.
 *
 * java case 06
 * Flink DataStream 训练 - 聚合函数训练
 */
public class FlinkDataStreamTraining_Agg {

    private static final Logger logger  = LoggerFactory.getLogger(FlinkDataStreamTraining_Agg.class);

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        DataStreamSource<Tuple3<String, String, Timestamp>> clickStream = env.addSource(new OutOfOrderDataSource());
        KeyedStream<Tuple3<String, String, Timestamp>, Tuple> keyedStream = clickStream.assignTimestampsAndWatermarks(
                new BoundedOutOfOrdernessTimestampExtractor<Tuple3<String, String, Timestamp>>(Time.seconds(10)) {
            @Override
            public long extractTimestamp(Tuple3<String, String, Timestamp> element) {
                return element.f2.getTime();
            }


        }).keyBy(0);

        SingleOutputStreamOperator<Tuple2<String, Long>> sum = keyedStream.timeWindow(Time.seconds(10))
                .aggregate(new AggregateFunction<Tuple3<String, String, Timestamp>, Tuple2<String, Long>, Tuple2<String, Long>>() {

                    @Override
                    public Tuple2<String, Long> createAccumulator() {
                        return new Tuple2<String, Long>("", 0L);
                    }

                    @Override
                    public Tuple2<String, Long> add(Tuple3<String, String, Timestamp> value, Tuple2<String, Long> accumulator) {
                        Tuple2<String, Long> tuple2 = new Tuple2<>(value.f0, accumulator.f1 + 1);
                        return tuple2;
                    }

                    @Override
                    public Tuple2<String, Long> getResult(Tuple2<String, Long> accumulator) {
                        return accumulator;
                    }

                    @Override
                    public Tuple2<String, Long> merge(Tuple2<String, Long> a, Tuple2<String, Long> b) {
                        Tuple2<String, Long> tuple2 = new Tuple2<>(a.f0, a.f1 + b.f1);
                        return tuple2;
                    }
                });

        sum.addSink(new SinkFunction<Tuple2<String, Long>>() {
            @Override
            public void invoke(Tuple2<String, Long> value, Context context) throws Exception {
                logger.info("print {}", value);
            }
        });

        logger.info("execute flink job......");

        String jobName = env.getStreamGraph().getJobName();
        logger.info("job name is {}", jobName);

        env.execute("Flink DataStram Training Aggregate");

        ExecutionConfig config = env.getConfig();
        System.out.println(config);
    }

}
