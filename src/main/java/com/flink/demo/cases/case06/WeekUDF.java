package com.flink.demo.cases.case06;

import org.apache.flink.table.functions.ScalarFunction;

import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.time.temporal.WeekFields;

/**
 * Created by P0007 on 2019/11/4.
 * Nokia定制，时间转换相关UDF
 */
public class WeekUDF extends ScalarFunction {

    /**
     * 获取timestamp在这一年中是第几周
     * @param timestamp
     * @return
     */
    public int eval(Timestamp timestamp) {
        LocalDateTime localDateTime = timestamp.toLocalDateTime();
        int week = localDateTime.get(WeekFields.ISO.weekOfWeekBasedYear());
        return week;
    }

}
