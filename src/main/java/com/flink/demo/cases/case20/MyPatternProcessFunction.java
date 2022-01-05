package com.flink.demo.cases.case20;

import org.apache.flink.cep.functions.PatternProcessFunction;
import org.apache.flink.cep.functions.TimedOutPartialMatchHandler;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import java.util.List;
import java.util.Map;

/**
 * Created by P0007 on 2019/12/6.
 */
public class MyPatternProcessFunction
        extends PatternProcessFunction<Row, Row>
        implements TimedOutPartialMatchHandler<Row> {

    @Override
    public void processMatch(Map<String, List<Row>> match, Context ctx, Collector<Row> out) throws Exception {
        System.out.println(match);
    }

    @Override
    public void processTimedOutMatch(Map<String, List<Row>> match, Context ctx) throws Exception {
        System.err.println(match);
    }
}
