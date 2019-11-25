package com.flink.demo.cases.case23;

import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimerService;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Created by P0007 on 2019/10/28.
 */
public class FlinkIntarvalFilterProcessFunction extends ProcessFunction<Tuple4<Integer, String, String, Timestamp>,
        Tuple4<Integer, String, String, Timestamp>> {

    private static final Logger logger = LoggerFactory.getLogger(FlinkIntarvalFilterProcessFunction.class);

    private static final String BUFFER = "BUFFER";

    private transient MapState<Long, List<Tuple4<Integer, String, String, Timestamp>>> buffer;

    private final long lowerBound;
    private final long upperBound;


    public FlinkIntarvalFilterProcessFunction(long lowerBound, long upperBound,
                                              boolean lowerBoundInclusive, boolean upperBoundInclusive) {
        Preconditions.checkArgument(lowerBound <= upperBound,
                "lowerBound <= upperBound must be fulfilled");

        // Move buffer by +1 / -1 depending on inclusiveness in order not needing
        // to check for inclusiveness later on
        this.lowerBound = (lowerBoundInclusive) ? lowerBound : lowerBound + 1L;
        this.upperBound = (upperBoundInclusive) ? upperBound : upperBound - 1L;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);

        TypeInformation<Long> keyInfo = TypeInformation.of(Long.class);
        TypeInformation<List<Tuple4<Integer, String, String, Timestamp>>> valueInfo = TypeInformation.of(
                new TypeHint<List<Tuple4<Integer, String, String, Timestamp>>>() {
                });
        MapStateDescriptor<Long, List<Tuple4<Integer, String, String, Timestamp>>> stateDescriptor = new MapStateDescriptor<>(
                BUFFER, keyInfo, valueInfo);
        this.buffer = getRuntimeContext().getMapState(stateDescriptor);
    }

    @Override
    public void processElement(Tuple4<Integer, String, String, Timestamp> value,
                               Context ctx,
                               Collector<Tuple4<Integer, String, String, Timestamp>> out) throws Exception {

        TimerService timerService = ctx.timerService();
        long timestamp = ctx.timestamp();
        if (timestamp == Long.MIN_VALUE) {
            throw new FlinkException("Long.MIN_VALUE timestamp: Elements used in " +
                    "interval stream filter need to have timestamps meaningful timestamps.");
        }

        if (isLate(timerService, timestamp)) {
            return;
        }

        boolean emit = true;
        for (Map.Entry<Long, List<Tuple4<Integer, String, String, Timestamp>>> bucket : buffer.entries()) {
            logger.info("Compare bucket: {}", bucket);
            final Long bucketTimestamp = bucket.getKey();

            if (bucketTimestamp < timestamp + lowerBound ||
                    bucketTimestamp > timestamp + upperBound) {
                logger.info("bucketTimestamp should be skipped. timestamp is {}, bucketTimestamp is {}", timestamp, bucketTimestamp);
                continue;
            }

            List<Tuple4<Integer, String, String, Timestamp>> bucketes = bucket.getValue();

            for (Tuple4<Integer, String, String, Timestamp> bucketCache : bucketes) {
                if (bucketCache.f2.equals(value.f2)) {
                    logger.info("match {} \n {}", value, bucket);
                    emit = false;
                    break;
                }
            }
            if (emit == false) {
                break;
            }
        }
        if (emit) {
            out.collect(value);
        }

        long cleanupTime = timestamp + Math.abs(lowerBound);
        logger.info("Register cleanup timer: {}", cleanupTime);
        timerService.registerEventTimeTimer(cleanupTime);

        addToBuffer(buffer, value, timestamp);
    }

    private boolean isLate(TimerService timerService, long timestamp) {
        long currentWatermark = timerService.currentWatermark();
        return currentWatermark != Long.MIN_VALUE && timestamp < currentWatermark;
    }

    private static <T> void addToBuffer(
            final MapState<Long, List<T>> buffer,
            final T value,
            final long timestamp) throws Exception {
        List<T> elemsInBucket = buffer.get(timestamp);
        if (elemsInBucket == null) {
            elemsInBucket = new ArrayList<>();
        }
        elemsInBucket.add(value);
        logger.info("Add {} - {}", timestamp, elemsInBucket);
        buffer.put(timestamp, elemsInBucket);
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<Tuple4<Integer, String, String, Timestamp>> out) throws Exception {
        logger.info("onEventTime @ {}", timestamp);
        long bufferTimestamp = (upperBound <= 0L) ? timestamp : timestamp - upperBound;
        logger.info("Removing from buffer @ {}", bufferTimestamp);
        buffer.remove(bufferTimestamp);
    }
}
