package com.flink.demo.nokia.jinke.udf;

import org.apache.flink.table.functions.ScalarFunction;

import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * Created by P0007 on 2020/3/5.
 */
public class MinuteFunction extends ScalarFunction {

    public String eval(Timestamp timestamp, String format) {
        SimpleDateFormat dateFormat = new SimpleDateFormat(format);
        long time = timestamp.getTime();
        String date = dateFormat.format(new Date(time));
        return date;
    }

}
