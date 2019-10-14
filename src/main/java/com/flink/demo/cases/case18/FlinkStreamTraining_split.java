package com.flink.demo.cases.case18;

import com.flink.demo.cases.common.datasource.UrlClickRowDataSource;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

public class FlinkStreamTraining_split {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        DataStream<Row> urlClickStream = env
                .addSource(new UrlClickRowDataSource())
                .returns(UrlClickRowDataSource.USER_CLICK_TYPEINFO);

        OutputTag<Row> outputTag65 = new OutputTag<Row>("userId-65", UrlClickRowDataSource.USER_CLICK_TYPEINFO);
        OutputTag<Row> outputTag66 = new OutputTag<Row>("userId-66", UrlClickRowDataSource.USER_CLICK_TYPEINFO);

        SingleOutputStreamOperator<Row> processedStream = urlClickStream.process(new ProcessFunction<Row, Row>() {
            @Override
            public void processElement(Row value, Context ctx, Collector<Row> out) throws Exception {
                int userId = Integer.parseInt(value.getField(0).toString());
                if (userId == 65) {
                    ctx.output(outputTag65, value);
                } else if (userId == 66) {
                    ctx.output(outputTag66, value);
                } else {
                    out.collect(value);
                }
            }
        });

        processedStream.printToErr("main-out");

        DataStream<Row> sideOutput65 = processedStream.getSideOutput(outputTag65);
        sideOutput65.printToErr("sideout-65");

        DataStream<Row> sideOutput66 = processedStream.getSideOutput(outputTag66);
        sideOutput66.printToErr("sideout-66");

        env.execute("Flink Stream Sideout Training");

    }

}
