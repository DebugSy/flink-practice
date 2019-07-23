package com.flink.demo.cases.common.watermark;

import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.time.Time;

import javax.annotation.Nullable;

/**
 * Created by DebugSy on 2019/7/23.
 */
public class CustomTimestampExtractor<T> implements AssignerWithPeriodicWatermarks<T> {

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
    public long extractTimestamp(T element, long previousElementTimestamp) {
        return 0;
    }
}
