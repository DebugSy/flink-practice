package com.flink.demo.cases.common.functions.udf;

import org.apache.flink.table.functions.ScalarFunction;

import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * Created by P0007 on 2019/8/20.
 */
public class Timestamp2Timezone extends ScalarFunction {

    public String eval(Timestamp timestamp, String dateFormat) {
        SimpleDateFormat formater = new SimpleDateFormat(dateFormat);
        Date date = new Date(timestamp.getTime());
        String dateStr = formater.format(date);
        return dateStr;
    }

}
