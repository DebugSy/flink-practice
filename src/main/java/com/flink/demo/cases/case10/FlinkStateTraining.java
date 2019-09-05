package com.flink.demo.cases.case10;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.util.Collector;

import java.util.Random;

/**
 * Created by P0007 on 2019/9/4.
 */
public class FlinkStateTraining {

    public static void main(String[] args) throws Exception {
        Configuration configuration = new Configuration();
        configuration.setString("queryable-state.enable", "true");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment(1, configuration);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(1);
        env.enableCheckpointing(1000 * 5);
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(500);
        env.getCheckpointConfig().setCheckpointTimeout(1000 * 60);
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        env.setStateBackend((StateBackend)new RocksDBStateBackend("file:///tmp/flink-checkpoints/"));

        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, Time.seconds(10)));


        // this can be used in a streaming program like this (assuming we have a StreamExecutionEnvironment env)
        env.addSource(new DataSource()).keyBy(0)
                .flatMap(new CountWindowAverage())
                .printToErr();

        // the printed output will be (1,4) and (1,5)

        env.execute("Flink state training");

    }

}

class DataSource extends RichSourceFunction<Tuple2<Long, Long>> {

    private boolean running = true;

    @Override
    public void run(SourceContext<Tuple2<Long, Long>> ctx) throws Exception {
        Random random = new Random(System.currentTimeMillis());
        while (running) {
            long f0 = random.nextInt(3);
            long f1 = random.nextInt(20);
            Tuple2<Long, Long> tuple2 = new Tuple2<>(f0, f1);
            System.out.println("emit -> " + tuple2);
            ctx.collect(tuple2);

            Thread.sleep(3000);
        }
    }

    @Override
    public void cancel() {
        running = false;
    }
}

class CountWindowAverage extends RichFlatMapFunction<Tuple2<Long, Long>, Tuple2<Long, Long>> {

    /**
     * The ValueState handle. The first field is the count, the second field a running sum.
     */
    private transient ValueState<Tuple2<Long, Long>> sum;

    @Override
    public void flatMap(Tuple2<Long, Long> input, Collector<Tuple2<Long, Long>> out) throws Exception {

        // access the state value
        Tuple2<Long, Long> currentSum = sum.value();
        if (currentSum == null) {
            currentSum = new Tuple2<>(0L, 0L);
        }

        // update the count
        currentSum.f0 += 1;

        // add the second field of the input value
        currentSum.f1 += input.f1;

        // update the state
        sum.update(currentSum);

        // if the count reaches 2, emit the average and clear the state
        if (currentSum.f0 >= 2) {
            out.collect(new Tuple2<>(input.f0, currentSum.f1 / currentSum.f0));
            sum.clear();
        }
    }

    @Override
    public void open(Configuration config) {
        ValueStateDescriptor<Tuple2<Long, Long>> descriptor =
                new ValueStateDescriptor<>(
                        "average", // the state name
                        TypeInformation.of(new TypeHint<Tuple2<Long, Long>>() {})); // default value of the state, if nothing was set
        descriptor.setQueryable("query-name");
        sum = getRuntimeContext().getState(descriptor);
    }
}
