package com.flink.demo.cases.common.datasource;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Timestamp;
import java.util.Random;

/**
 * Created by DebugSy on 2019/7/15.
 *
 * 用户点击网页事件数据源
 */
public class UrlClickDataSource extends RichParallelSourceFunction<Tuple3<String, String, Timestamp>> {

    private static final Logger logger = LoggerFactory.getLogger(UrlClickDataSource.class);

    private volatile boolean running = true;

    public static String CLICK_FIELDS = "username,url,clickTime";

    public static String CLICK_FIELDS_WITH_ROWTIME = "username,url,clickTime,clickActionTime.rowtime";

    @Override
    public void run(SourceContext<Tuple3<String, String, Timestamp>> ctx) throws Exception {
        Random random = new Random(System.currentTimeMillis());
        long count = 0;
        while (running) {
            int indexOfThisSubtask = getRuntimeContext().getIndexOfThisSubtask();
//            logger.info("The index of the parallel subtask is {}", indexOfThisSubtask);
            Thread.sleep((indexOfThisSubtask + 1) * random.nextInt(10) * random.nextInt(100));
            String username = "用户" + (char) ('A' + random.nextInt(5));
            Timestamp clickTime = new Timestamp(System.currentTimeMillis());
            String url = "http://127.0.0.1/api/" + (char) ('H' + random.nextInt(4));
            Tuple3<String, String, Timestamp> tuple3 = new Tuple3<>(username, url, clickTime);
            count++;
            logger.info("emit -> {}, count is {}", tuple3, count);
//                ctx.collectWithTimestamp(tuple3, clickTime.getTime());
//                ctx.emitWatermark(new Watermark(clickTime.getTime()));
            ctx.collect(tuple3);
        }
    }

    @Override
    public void cancel() {
        running = false;
    }

}
