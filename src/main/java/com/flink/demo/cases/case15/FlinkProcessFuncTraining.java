package com.flink.demo.cases.case15;

import com.flink.demo.cases.common.datasource.UrlClickRowDataSource;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

/**
 * Created by P0007 on 2019/9/18.
 */
public class FlinkProcessFuncTraining {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        DataStreamSource<Row> streamSource = env.addSource(new UrlClickRowDataSource());
        streamSource.process(new ProcessFunction<Row, Object>() {

            final Row resultRow = new Row(3);

            @Override
            public void processElement(Row value, Context ctx, Collector<Object> out) throws Exception {
                System.err.println("print: " + value);
                Object field = value.getField(1);
                String field1 = (String) field;
                java.lang.String result$0 = "UserA";
                boolean result$1 = field1.compareTo(result$0) == 0;
                if (result$1) {
                    out.collect(resultRow);
                }
            }
        });

        env.execute("Flink ProcessFunction Training");
    }

}
