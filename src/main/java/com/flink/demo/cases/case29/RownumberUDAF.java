package com.flink.demo.cases.case29;

import org.apache.flink.table.functions.AggregateFunction;
import org.apache.flink.types.Row;

import java.util.SortedMap;

public class RownumberUDAF extends AggregateFunction<Row, RownumberAccumulator> {

    @Override
    public RownumberAccumulator createAccumulator() {
        return new RownumberAccumulator();
    }

    public void accumulate(RownumberAccumulator accumulator, Row row){
        SortedMap<Row, Long> treeMap = accumulator.getTreeMap();
        if (treeMap.containsKey(row)) {
            treeMap.put(row, treeMap.get(row) + 1);
        } else {
            treeMap.put(row, 1L);
        }
    }

    @Override
    public Row getValue(RownumberAccumulator accumulator) {
        return accumulator.getTreeMap().firstKey();
    }


}
