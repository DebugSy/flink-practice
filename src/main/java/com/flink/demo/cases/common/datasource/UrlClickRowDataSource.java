package com.flink.demo.cases.common.datasource;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.table.api.Types;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Timestamp;
import java.util.Random;

/**
 * Created by P0007 on 2019/9/3.
 */
public class UrlClickRowDataSource extends RichParallelSourceFunction<Row> {

    private static final Logger logger = LoggerFactory.getLogger(UrlClickDataSource.class);

    private volatile boolean running = true;

    public static String CLICK_FIELDS = "userId,username,url,clickTime";

    public static String CLICK_FIELDS_WITH_ROWTIME = "userId,username,url,clickTime,rowtime.rowtime";

    public static TypeInformation USER_CLICK_TYPEINFO = Types.ROW(
            new String[]{"userId", "username", "url", "clickTime"},
            new TypeInformation[]{
                    Types.INT(),
                    Types.STRING(),
                    Types.STRING(),
                    Types.SQL_TIMESTAMP()
            });

    @Override
    public void run(SourceContext<Row> ctx) throws Exception {
        Random random = new Random(System.currentTimeMillis());
        while (running) {
            int indexOfThisSubtask = getRuntimeContext().getIndexOfThisSubtask();
            Thread.sleep((indexOfThisSubtask + 1) * 1000);
            int nextInt = random.nextInt(5);
            Integer userId = 65 + nextInt;
            String username = "user " + (char) ('A' + nextInt);
            String url = "http://www.inforefiner.com/api/" + (char) ('H' + random.nextInt(4));
            Timestamp clickTime = new Timestamp(System.currentTimeMillis());
            Row row = new Row(4);
            row.setField(0, userId);
            row.setField(1, username);
            row.setField(2, url);
            row.setField(3, clickTime);
            logger.info("emit -> {}", row);
            ctx.collectWithTimestamp(row, clickTime.getTime());
            ctx.emitWatermark(new Watermark(clickTime.getTime()));
        }
    }

    @Override
    public void cancel() {
        running = false;
    }

}

