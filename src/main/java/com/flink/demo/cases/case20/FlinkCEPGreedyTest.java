package com.flink.demo.cases.case20;

import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.functions.PatternProcessFunction;
import org.apache.flink.cep.nfa.aftermatch.AfterMatchSkipStrategy;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.List;
import java.util.Map;

/**
 * Created by P0007 on 2019/12/2.
 */
public class FlinkCEPGreedyTest {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStream<Integer> input = env.fromElements(1, 1, 1, 1, 1, 0, 1, 1, 1, 0);

        Pattern<Integer, ?> onesThenZero = Pattern.<Integer>begin("ones", AfterMatchSkipStrategy.skipPastLastEvent())
                .where(new SimpleCondition<Integer>() {
                    @Override
                    public boolean filter(Integer value) throws Exception {
                        return value == 1;
                    }
                })
                .oneOrMore()
                .greedy()

                .next("zero")
                .where(new SimpleCondition<Integer>() {
                    @Override
                    public boolean filter(Integer value) throws Exception {
                        return value == 0;
                    }
                });

        PatternStream<Integer> pattern = CEP.pattern(input, onesThenZero);

        // Expected: 5 3
        // Actual: 5 4 3 2 1 3 2 1
        pattern.process(new PatternProcessFunction<Integer, Object>() {
            @Override
            public void processMatch(Map<String, List<Integer>> match, Context ctx, Collector<Object> out) throws Exception {
                System.err.println(match);
            }
        });

        env.execute();
    }

}
