package com.flink.demo.cases.case06;

import org.apache.flink.table.functions.ScalarFunction;

import java.sql.Time;
import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.*;
import java.time.format.DateTimeFormatter;
import java.util.Date;
import java.util.Locale;

/**
 * Created by P0007 on 2020/3/16.
 */
public class TimeUDF extends ScalarFunction {

    public Timestamp eval(String date, String format) {
        SimpleDateFormat dateFormat = new SimpleDateFormat(format);
        try {
            Date parsedDate = dateFormat.parse(date);
            return new Timestamp(parsedDate.getTime());
        } catch (ParseException e) {
            e.printStackTrace();
            throw new RuntimeException("Parser data "+ date +" with format "+ format +" throw exception", e);
        }
    }

    public Timestamp eval(Timestamp timestamp, int diff) {
        long l = timestamp.getTime() + diff;
        return new Timestamp(l);
    }

    public String eval(Timestamp timestamp, String format) {
        DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern(format);
        LocalDateTime localDateTime = timestamp.toLocalDateTime();
        ZonedDateTime zonedDateTime = localDateTime.atZone(ZoneId.systemDefault());
        String format1 = localDateTime.atZone(ZoneId.systemDefault()).format(dateTimeFormatter);
        return format1;
    }


    /**
     * 获取当前时间的毫秒值，带参数是为了防止sql优化做常量折叠
     * @param timestamp
     * @return
     */
    public long eval(String timestamp) {
        return System.currentTimeMillis();
    }

}
