package com.flink.demo.cases.case20;

import com.flink.demo.cases.common.datasource.UrlClickRowDataSource;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.functions.PatternProcessFunction;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import java.util.List;
import java.util.Map;

/**
 * Created by P0007 on 2019/10/18.
 */
public class FlinkCEPTraining {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        SingleOutputStreamOperator sourceStream = env.addSource(new UrlClickRowDataSource())
                .returns(UrlClickRowDataSource.USER_CLICK_TYPEINFO);

        //build
        Pattern<Row, Row> pattern = Pattern.<Row>begin("start")
                .where(new SimpleCondition<Row>() {
                    @Override
                    public boolean filter(Row value) throws Exception {
                        return Integer.parseInt(value.getField(0).toString()) == 65;
                    }
                })
                .next("middle")
                .where(new SimpleCondition<Row>() {
                    @Override
                    public boolean filter(Row value) throws Exception {
                        return Integer.parseInt(value.getField(0).toString()) == 65;
                    }
                })
                .followedBy("end")
                .where(new SimpleCondition<Row>() {
                    @Override
                    public boolean filter(Row value) throws Exception {
                        return Integer.parseInt(value.getField(0).toString()) == 65;
                    }
                });

        PatternStream<Row> patternStream = CEP.pattern(sourceStream, pattern);

        patternStream.process(new PatternProcessFunction<Row, Row>() {
            @Override
            public void processMatch(Map<String, List<Row>> match, Context ctx, Collector<Row> out) throws Exception {
                System.err.println(match);
            }
        });


        env.execute("FLink CEP Training");
    }

}
