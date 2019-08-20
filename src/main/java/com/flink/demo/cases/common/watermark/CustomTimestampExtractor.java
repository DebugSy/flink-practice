package com.flink.demo.cases.common.watermark;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.time.Time;

import javax.annotation.Nullable;
import java.sql.Timestamp;

/**
 * Created by DebugSy on 2019/7/23.
 */
public class CustomTimestampExtractor implements AssignerWithPeriodicWatermarks<Tuple3<String, String, Timestamp>> {

    /**
     * 由于flink时间与北京时间差8 hours，在指定水印时增加8小时
     */
    private static final long LATENCY_8_HOURS = 28800000;

    private long currentTimeStamp = Long.MIN_VALUE;

    private final long maxOutOfOrderness;

    public CustomTimestampExtractor(Time maxOutOfOrderness) {
        this.maxOutOfOrderness = maxOutOfOrderness.toMilliseconds();
    }

    @Nullable
    @Override
    public Watermark getCurrentWatermark() {
        return new Watermark(currentTimeStamp == Long.MIN_VALUE ? Long.MIN_VALUE : currentTimeStamp - maxOutOfOrderness);
    }

    @Override
    public long extractTimestamp(Tuple3<String, String, Timestamp> element, long previousElementTimestamp) {
        long time = element.f2.getTime() + LATENCY_8_HOURS;
        currentTimeStamp = time;
        return time;
    }
}
