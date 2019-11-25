package com.flink.demo.cases.case06;

import org.apache.flink.table.functions.AggregateFunction;

import java.util.Iterator;

/**
 * Created by P0007 on 2019/10/23.
 */
public class MyUDAF extends AggregateFunction<String, MyUDAF.AggString> {


    @Override
    public AggString createAccumulator() {
        return new AggString();
    }

    @Override
    public String getValue(AggString accumulator) {
        return accumulator.string;
    }

    public void accumulate(AggString acc, String s2) {
        acc.string = acc.string.concat(s2);
    }

    public void merge(AggString acc, Iterable<AggString> t2) {
        Iterator<AggString> iterator = t2.iterator();
        while (iterator.hasNext()) {
            String s2 = iterator.next().string;
            acc.string = acc.string.concat(s2);
        }
    }

    public static class AggString {
        public String string = "";
    }

}
