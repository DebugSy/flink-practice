package com.flink.demo.cases.common.datasource;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

/**
 * 乱序数据源
 * 按顺序写出数据，时间错乱，未指定时间戳和水印
 */
public class OutOfOrderDataSource extends RichSourceFunction<Tuple3<String, String, Timestamp>> {

    private static final Logger logger = LoggerFactory.getLogger(OutOfOrderDataSource.class);

    public static String CLICK_FIELDS = "userId,username,url,clickTime,rowtime.rowtime";

    private boolean running = true;

    private static List<List<String>> clicks = new ArrayList<>();

    static {
        clicks.add(Arrays.asList("65", "用户A", "http://127.0.0.1/api/I", "2019-07-23 23:27:29.876"));
        clicks.add(Arrays.asList("", "用户E", "http://127.0.0.1/api/H", "2019-07-23 23:27:31.893"));
        clicks.add(Arrays.asList("", "用户C", "http://127.0.0.1/api/I", "2019-07-23 23:27:32.897"));
        clicks.add(Arrays.asList("", "用户B", "http://127.0.0.1/api/H", "2019-07-23 23:27:36.908"));
        clicks.add(Arrays.asList("", "用户A", "http://127.0.0.1/api/J", "2019-07-23 23:27:27.019"));//晚到数据
        clicks.add(Arrays.asList("", "用户E", "http://127.0.0.1/api/J", "2019-07-23 23:27:38.916"));
        clicks.add(Arrays.asList("", "用户A", "http://127.0.0.1/api/J", "2019-07-23 23:27:27.189"));//晚到数据
        clicks.add(Arrays.asList("", "用户B", "http://127.0.0.1/api/J", "2019-07-23 23:27:48.950"));
        clicks.add(Arrays.asList("", "用户E", "http://127.0.0.1/api/K", "2019-07-23 23:27:53.960"));
        clicks.add(Arrays.asList("", "用户C", "http://127.0.0.1/api/J", "2019-07-23 23:28:02.990"));
        clicks.add(Arrays.asList("", "用户A", "http://127.0.0.1/api/K", "2019-07-23 23:27:30.960"));//晚到数据
        clicks.add(Arrays.asList("", "用户E", "http://127.0.0.1/api/K", "2019-07-23 23:28:08.012"));
        clicks.add(Arrays.asList("", "用户B", "http://127.0.0.1/api/K", "2019-07-23 23:28:12.029"));
        clicks.add(Arrays.asList("", "用户E", "http://127.0.0.1/api/I", "2019-07-23 23:27:39.918"));//晚到数据
        clicks.add(Arrays.asList("", "用户C", "http://127.0.0.1/api/K", "2019-07-23 23:27:43.931"));//晚到数据
        clicks.add(Arrays.asList("", "用户B", "http://127.0.0.1/api/K", "2019-07-23 23:28:20.000"));
        clicks.add(Arrays.asList("", "用户C", "http://127.0.0.1/api/K", "2019-07-23 23:28:20.001"));
    }

    @Override
    public void run(SourceContext<Tuple3<String, String, Timestamp>> sourceContext) throws Exception {
        Iterator<List<String>> iterator = clicks.iterator();
        while (iterator.hasNext()) {
            List<String> record = iterator.next();
            Tuple3<String, String, Timestamp> tuple3 = new Tuple3<>(record.get(0), record.get(1),
                    Timestamp.valueOf(record.get(2)));
            logger.info("emit {}", tuple3);
            sourceContext.collect(tuple3);

            Thread.sleep(1000 * 1);
        }
    }

    @Override
    public void cancel() {
        running = false;
    }
}