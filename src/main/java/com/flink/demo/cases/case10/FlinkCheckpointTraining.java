package com.flink.demo.cases.case10;

import com.flink.demo.cases.common.datasource.UrlClickRowDataSource;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.types.Row;

import java.sql.Timestamp;

/**
 * Created by P0007 on 2019/11/1.
 */
public class FlinkCheckpointTraining {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(1);

        //checkpoint config
        env.enableCheckpointing(1000 * 5);
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(500);
        env.getCheckpointConfig().setCheckpointTimeout(1000 * 10);
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        env.setStateBackend((StateBackend)new FsStateBackend("file:///tmp/flink/checkpint-dir"));

        //restart strategy
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, org.apache.flink.api.common.time.Time.seconds(10)));

        //latency
        env.getConfig().setLatencyTrackingInterval(1000 * 5);

        SingleOutputStreamOperator<Row> urlclickSource = env.addSource(new UrlClickRowDataSource())
                .returns(UrlClickRowDataSource.USER_CLICK_TYPEINFO)
                .setParallelism(1)
                .name("Url Click Source");

        SingleOutputStreamOperator<Row> timestampsAndWatermarks = urlclickSource.assignTimestampsAndWatermarks(new AscendingTimestampExtractor<Row>() {
            @Override
            public long extractAscendingTimestamp(Row element) {
                return Timestamp.valueOf(element.getField(3).toString()).getTime();
            }
        });

        SingleOutputStreamOperator aggregate = timestampsAndWatermarks
                .keyBy(0)
                .timeWindow(Time.seconds(5), Time.seconds(10))
                .aggregate(new AggregateFunction<Row, Double, Row>() {

                    final Row row = new Row(1);

                    @Override
                    public Double createAccumulator() {
                        return new Double(0);
                    }

                    @Override
                    public Double add(Row value, Double accumulator) {
                        accumulator += Double.parseDouble(value.getField(0).toString());
                        return accumulator;
                    }

                    @Override
                    public Row getResult(Double accumulator) {
                        row.setField(0, accumulator);
                        return row;
                    }

                    @Override
                    public Double merge(Double a, Double b) {
                        double merge = a + b;
                        return merge;
                    }
                })
                .setParallelism(1)
                .name("Aggregate");

        aggregate.printToErr();

        env.execute("Flink Checkpoint Training");


    }

}
