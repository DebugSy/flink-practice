package com.flink.demo.cases.common.datasource;

import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Timestamp;
import java.util.Random;

/**
 * Created by DebugSy on 2019/7/15.
 */
public class UserDataSource extends RichSourceFunction<Tuple4<Integer, String, String, Timestamp>> {

    private static final Logger logger = LoggerFactory.getLogger(UserDataSource.class);

    private volatile boolean running = true;

    @Override
    public void run(SourceContext<Tuple4<Integer, String, String, Timestamp>> ctx) throws Exception {
        Random random = new Random(System.currentTimeMillis());
        while (running) {
            Thread.sleep(1000);
            int nextInt = random.nextInt(5);
            String username = "用户" + (char) ('A' + nextInt);
            Integer userId = 65 + nextInt;
            String address = "北京市朝阳区望京东湖街道" + nextInt + "号";
            Timestamp activityTime = new Timestamp(System.currentTimeMillis());
//            ctx.collect(new Tuple4<>(userId, username, address, activityTime));

            Tuple4<Integer, String, String, Timestamp> tuple4 = new Tuple4<>(userId, username, address, activityTime);
            logger.info("emit -> {}", tuple4);
            //直接在数据流源中指定时间戳和水印
            ctx.collectWithTimestamp(tuple4, activityTime.getTime());
        }
    }

    @Override
    public void cancel() {
        running = false;
    }
}
