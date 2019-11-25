package com.flink.demo.cases.case20;

import org.apache.flink.table.functions.ScalarFunction;

import java.sql.Timestamp;

/**
 * Created by P0007 on 2019/11/4.
 */
public class TimeUDF extends ScalarFunction {

    public long eval(String timestamp) {
        return System.currentTimeMillis();
    }

    public long eval(Timestamp timestamp) {
        return timestamp.getTime();
    }

}
