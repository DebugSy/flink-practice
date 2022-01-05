package com.flink.demo.cases.case29;

import org.apache.flink.table.functions.AggregateFunction;

public class SumUDAF extends AggregateFunction<Integer, Integer> {

    @Override
    public Integer createAccumulator() {
        return new Integer(0);
    }

    public void accumulate(Integer acc, Integer integer){
        acc += integer;
    }

    @Override
    public Integer getValue(Integer accumulator) {
        return accumulator;
    }
}
