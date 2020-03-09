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
 * <p>
 * 比较 greedy的差别
 * 1: 用在followedBy与next中间，无效果
 * 2: 用在followedBy与followedBy中间有效
 */
public class FlinkCEPGreedyTest {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStream<Integer> input = env.fromElements(10, 21, 22, 23, 24, 11, 10, 11);

        Pattern<Integer, ?> onesThenZero =
                Pattern.<Integer>begin("ones", AfterMatchSkipStrategy.noSkip())
                        .where(new SimpleCondition<Integer>() {
                            @Override
                            public boolean filter(Integer value) throws Exception {
                                return value == 10;
                            }
                        })

                        .followedBy("middle")
                        .where(new SimpleCondition<Integer>() {
                            @Override
                            public boolean filter(Integer value) throws Exception {
                                return value > 20;
                            }
                        })
                        .times(2)
//                        .consecutive()
//                        .optional()
//                        .greedy()
//                        .until(wnew SimpleCondition<Integer>() {
//                            @Override
//                            public boolean filter(Integer value) throws Exception {
//                                return value == 24;
//                            }
//                        })

                        .followedBy("end")
                        .where(new SimpleCondition<Integer>() {
                            @Override
                            public boolean filter(Integer value) throws Exception {
                                return value == 11;
                            }
                        })
                ;

        PatternStream<Integer> pattern = CEP.pattern(input, onesThenZero);

        pattern.process(new PatternProcessFunction<Integer, Object>() {
            @Override
            public void processMatch(Map<String, List<Integer>> match, Context ctx, Collector<Object> out) throws Exception {
                System.err.println(match);
            }
        });

        env.execute();
    }

}
