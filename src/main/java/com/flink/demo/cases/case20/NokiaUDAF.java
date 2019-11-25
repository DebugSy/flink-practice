package com.flink.demo.cases.case20;
import org.apache.flink.table.functions.AggregateFunction;
import org.apache.flink.types.Row;

import java.util.Iterator;

/**
 * Created by P0007 on 2019/11/4.
 */
public class NokiaUDAF extends AggregateFunction<Row, NokiaAcc> {

    private final Row row = new Row(2);

    @Override
    public NokiaAcc createAccumulator() {
        return new NokiaAcc(0, 0);
    }

    @Override
    public Row getValue(NokiaAcc accumulator) {
        row.setField(0, accumulator.total);
        row.setField(1, accumulator.empty);
        return row;
    }

    public void accumulate(NokiaAcc acc, Integer flag) {
        if (flag == null) {
            acc.empty++;
        }
       acc.total++;
    }

    public void merge(NokiaAcc acc1, Iterable<NokiaAcc> acc2s) {
        Iterator<NokiaAcc> iterator = acc2s.iterator();
        while (iterator.hasNext()) {
            NokiaAcc acc2 = iterator.next();
            acc1.total = acc1.total + acc2.total;
            acc1.empty = acc1.empty + acc2.empty;
        }
    }

}
