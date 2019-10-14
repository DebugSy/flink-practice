package com.flink.demo.cases.case06;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.windowing.RichWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

/**
 * Created by P0007 on 2019/10/9.
 */
public class FlinkWindowFunction extends RichWindowFunction<Row, Row, Row, TimeWindow> {

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
    }

    @Override
    public void apply(Row row, TimeWindow window, Iterable<Row> input, Collector<Row> out) throws Exception {
        System.out.println(row);
    }

}
