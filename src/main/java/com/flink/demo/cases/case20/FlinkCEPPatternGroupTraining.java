package com.flink.demo.cases.case20;

import com.flink.demo.cases.common.datasource.CEPOutOfOrderDataSource;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.functions.PatternProcessFunction;
import org.apache.flink.cep.nfa.aftermatch.AfterMatchSkipStrategy;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.util.List;
import java.util.Map;

/**
 * Created by P0007 on 2019/10/18.
 */
public class FlinkCEPPatternGroupTraining {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        DataStream<Row> sourceStream = env.addSource(
                new CEPOutOfOrderDataSource("src/main/resources/data/outoforder/CepOutOfOrderData.csv"))
                .returns(CEPOutOfOrderDataSource.CLICK_TYPEINFO);

        SingleOutputStreamOperator<Row> watermarks = sourceStream.assignTimestampsAndWatermarks(new AscendingTimestampExtractor<Row>() {
            @Override
            public long extractAscendingTimestamp(Row element) {
                return Timestamp.valueOf(element.getField(3).toString()).getTime();
            }
        });

        //build
        Pattern<Row, Row> groupPattern = Pattern.<Row>begin("group_start", AfterMatchSkipStrategy.skipToLast("start"))
                .where(new SimpleCondition<Row>() {
                    @Override
                    public boolean filter(Row value) throws Exception {
                        return Integer.parseInt(value.getField(0).toString()) == 68;
                    }
                })
                .followedBy("group_middle")
                .where(new SimpleCondition<Row>() {
                    @Override
                    public boolean filter(Row value) throws Exception {
                        return Integer.parseInt(value.getField(0).toString()) > 68;
                    }
                })
                .followedBy("group_end")
                .where(new SimpleCondition<Row>() {
                    @Override
                    public boolean filter(Row value) throws Exception {
                        return Integer.parseInt(value.getField(0).toString()) < 68;
                    }
                });

        Pattern<Row, Row> start = Pattern.<Row>begin("start")
                .where(new SimpleCondition<Row>() {
                    @Override
                    public boolean filter(Row value) throws Exception {
                        return Integer.parseInt(value.getField(0).toString()) == 68;
                    }
                })
                .followedBy(groupPattern)
                .times(2)
                .consecutive()
                .followedBy("end")
                .where(new SimpleCondition<Row>() {
                    @Override
                    public boolean filter(Row value) throws Exception {
                        return Integer.parseInt(value.getField(0).toString()) == 68;
                    }
                });

        PatternStream<Row> patternStream = CEP.pattern(watermarks, start);

        patternStream.process(new PatternProcessFunction<Row, Row>() {
            @Override
            public void processMatch(Map<String, List<Row>> match, Context ctx, Collector<Row> out) throws Exception {
                System.err.println(match);
            }
        });


        env.execute("FLink CEP Training");
    }

}
