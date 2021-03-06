package com.flink.demo.cases.case06;

import com.flink.demo.cases.common.datasource.OutOfOrderDataSource;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.runtime.state.heap.HeapPriorityQueueElement;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.OutputTag;
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

        DataStreamSource<Tuple4<Integer, String, String, Timestamp>> clickStream = env.addSource(new OutOfOrderDataSource());
        KeyedStream<Tuple4<Integer, String, String, Timestamp>, Tuple> keyedStream = clickStream.assignTimestampsAndWatermarks(
                new BoundedOutOfOrdernessTimestampExtractor<Tuple4<Integer, String, String, Timestamp>>(Time.seconds(0)) {
            @Override
            public long extractTimestamp(Tuple4<Integer, String, String, Timestamp> element) {
                return element.f3.getTime();
            }


        }).keyBy(0);

        OutputTag<Tuple4<Integer, String, String, Timestamp>> lateDataTag = new OutputTag<>("late_date", clickStream.getType());

        SingleOutputStreamOperator<Tuple2<String, Long>> sum = keyedStream.timeWindow(Time.seconds(5))
                .sideOutputLateData(lateDataTag)
                .aggregate(new AggregateFunction<Tuple4<Integer, String, String, Timestamp>, Tuple2<String, Long>, Tuple2<String, Long>>() {

                    @Override
                    public Tuple2<String, Long> createAccumulator() {
                        return new Tuple2<String, Long>("", 0L);
                    }

                    @Override
                    public Tuple2<String, Long> add(Tuple4<Integer, String, String, Timestamp> value, Tuple2<String, Long> accumulator) {
                        Tuple2<String, Long> tuple2 = new Tuple2<>(value.f1, accumulator.f1 + 1);
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

        sum.printToErr("mainDataTag");

        DataStream<Tuple4<Integer, String, String, Timestamp>> sideOutput = sum.getSideOutput(lateDataTag);
        sideOutput.printToErr("lateDataTag");

        logger.info("execute flink job......");

        String jobName = env.getStreamGraph().getJobName();
        logger.info("job name is {}", jobName);



        env.execute("Flink DataStram Training Aggregate");
    }

}
