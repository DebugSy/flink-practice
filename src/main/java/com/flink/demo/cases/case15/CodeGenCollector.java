package com.flink.demo.cases.case15;

import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

/**
 * Created by P0007 on 2019/9/24.
 */
public class CodeGenCollector implements Collector<Row> {
    
    private Collector<Row> out;
    
    @Override
    public void collect(Row record) {
        out.collect(record);
    }

    @Override
    public void close() {
        out.close();
    }
}
