package com.flink.demo.cases.case22;

import com.flink.demo.cases.common.datasource.UserDataSource;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;

/**
 * Created by P0007 on 2019/10/23.
 */
public class FlinkIntervalJoinTraining {

    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        DataStreamSource<Tuple4<Integer, String, String, Timestamp>> s1 = env.addSource(new UserDataSource());
        DataStreamSource<Tuple4<Integer, String, String, Timestamp>> s2 = env.addSource(new UserDataSource());

        s1.keyBy(1)
                .intervalJoin(s2.keyBy(1))
                .between(Time.seconds(-2), Time.seconds(0))
                .process(new ProcessJoinFunction<Tuple4<Integer, String, String, Timestamp>,
                        Tuple4<Integer, String, String, Timestamp>, Tuple4<Integer, String, String, Timestamp>>() {
                    @Override
                    public void processElement(Tuple4<Integer, String, String, Timestamp> left, Tuple4<Integer, String, String, Timestamp> right, Context ctx, Collector<Tuple4<Integer, String, String, Timestamp>> out) throws Exception {

                    }
                });
                
    }

}
