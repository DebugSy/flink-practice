package com.flink.demo.cases.case10;

import com.flink.demo.cases.common.datasource.UserDataSource;
import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * Created by P0007 on 2019/10/22.
 * <p>
 * java case 21 利用Flink State实现检测用户与之前行为的差别
 * 需求：检测用户当前地址是否包含在之前2小时去过的地址中
 */
public class FlinkStateTraining_filter {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        //checkpoint config
        env.enableCheckpointing(1000 * 5);
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(500);
        env.getCheckpointConfig().setCheckpointTimeout(1000 * 10);
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        env.setStateBackend((StateBackend)new FsStateBackend("file:///tmp/flink/checkpint-dir"));

        DataStreamSource<Tuple4<Integer, String, String, Timestamp>> streamSource = env.addSource(new UserDataSource());
        SingleOutputStreamOperator<Tuple4<Integer, String, String, Timestamp>> tuple4putStream = streamSource
                .assignTimestampsAndWatermarks(new AscendingTimestampExtractor<Tuple4<Integer, String, String, Timestamp>>() {
                    @Override
                    public long extractAscendingTimestamp(Tuple4<Integer, String, String, Timestamp> element) {
                        return element.f3.getTime();
                    }
                });

        KeyedStream<Tuple4<Integer, String, String, Timestamp>, Tuple> keyedStream = tuple4putStream.keyBy(1);

        SingleOutputStreamOperator<Tuple4<Integer, String, String, Timestamp>> sink = keyedStream
                .filter(new RichFilterFunction<Tuple4<Integer, String, String, Timestamp>>() {

                    MapState<String, List<Tuple2<String, Long>>> cacheState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        TypeInformation<String> keyInfo = TypeInformation.of(String.class);
                        TypeInformation<List<Tuple2<String, Long>>> valueInfo = TypeInformation.of(new TypeHint<List<Tuple2<String, Long>>>(){});
                        MapStateDescriptor stateDescriptor = new MapStateDescriptor("cache", keyInfo, valueInfo);
                        cacheState = getRuntimeContext().getMapState(stateDescriptor);
                    }

                    @Override
                    public boolean filter(Tuple4<Integer, String, String, Timestamp> value) throws Exception {
                        String username = value.f1;
                        boolean finded = false;
                        for (Map.Entry<String, List<Tuple2<String, Long>>> entry : cacheState.entries()) {
                            System.err.println(entry);
                        }
                        if (cacheState.contains(username)) {
                            List<Tuple2<String, Long>> tuple2 = cacheState.get(username);
                            Iterator<Tuple2<String, Long>> iterator = tuple2.iterator();
                            while (iterator.hasNext()) {
                                Tuple2<String, Long> cache = iterator.next();
                                if (cache.f1 >= value.f3.getTime() - 1000 * 5 && cache.f1 <= value.f3.getTime()) {
                                    finded = false;
                                } else {
                                    //clean state
                                    iterator.remove();
                                    finded = true;
                                }
                            }
                            tuple2.add(new Tuple2<>(value.f2, value.f3.getTime()));
                        } else {
                            List<Tuple2<String, Long>> adds = new ArrayList<>();
                            Tuple2<String, Long> tuple2 = new Tuple2<>(value.f2, value.f3.getTime());
                            adds.add(tuple2);
                            cacheState.put(username, adds);
                            finded = true;
                        }
                        return finded;
                    }
                });

        sink.printToErr();

        env.execute("Flink State Training");
    }

}
