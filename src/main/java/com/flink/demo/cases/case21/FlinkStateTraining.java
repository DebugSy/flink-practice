package com.flink.demo.cases.case21;

import com.flink.demo.cases.common.datasource.UserDataSource;
import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by P0007 on 2019/10/22.
 * <p>
 * java case 21 利用Flink State实现检测用户与之前行为的差别
 * 需求：检测用户当前地址是否包含在之前2小时去过的地址中
 */
public class FlinkStateTraining {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        DataStreamSource<Tuple4<Integer, String, String, Timestamp>> streamSource = env.addSource(new UserDataSource());
        SingleOutputStreamOperator<Tuple4<Integer, String, String, Timestamp>> tuple4putStream = streamSource
                .assignTimestampsAndWatermarks(new AscendingTimestampExtractor<Tuple4<Integer, String, String, Timestamp>>() {
                    @Override
                    public long extractAscendingTimestamp(Tuple4<Integer, String, String, Timestamp> element) {
                        return element.f3.getTime();
                    }
                });

        KeyedStream<Tuple4<Integer, String, String, Timestamp>, Tuple> keyedStream = tuple4putStream.keyBy(1);

        StateTtlConfig ttlConfig = StateTtlConfig
                .newBuilder(Time.seconds(5))
                .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
                .build();

        SingleOutputStreamOperator<Tuple4<Integer, String, String, Timestamp>> sink = keyedStream
                .filter(new RichFilterFunction<Tuple4<Integer, String, String, Timestamp>>() {

                    MapState<String, List<String>> cacheState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        MapStateDescriptor stateDescriptor = new MapStateDescriptor("cache", String.class, List.class);
                        stateDescriptor.enableTimeToLive(ttlConfig);
                        cacheState = getRuntimeContext().getMapState(stateDescriptor);
                    }

                    @Override
                    public boolean filter(Tuple4<Integer, String, String, Timestamp> value) throws Exception {
                        String username = value.f1;
                        if (cacheState.contains(username)) {
                            List<String> adds = cacheState.get(username);
                            if (adds.contains(value.f2)) {
                                return true;
                            }
                            adds.add(value.f2);
                        } else {
                            List<String> adds = new ArrayList<>();
                            adds.add(value.f2);
                            cacheState.put(username, adds);
                        }
                        return false;
                    }
                });

        sink.printToErr();

        env.execute("Flink State Training");
    }

}
