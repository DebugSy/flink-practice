
package com.flink.demo.cases.case23;

import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.runtime.state.VoidNamespace;
import org.apache.flink.runtime.state.VoidNamespaceSerializer;
import org.apache.flink.streaming.api.SimpleTimerService;
import org.apache.flink.streaming.api.TimerService;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.operators.*;
import org.apache.flink.streaming.api.operators.co.IntervalJoinOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.OutputTag;
import org.apache.flink.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * java case 23 基于Operator实现Interval Filter
 *
 * @param <K>
 */
public class IntervalFilterOperator<K>
        extends AbstractUdfStreamOperator<Tuple4<Integer, String, String, Timestamp>,
        ProcessFunction<Tuple4<Integer, String, String, Timestamp>, Tuple4<Integer, String, String, Timestamp>>>
        implements OneInputStreamOperator<Tuple4<Integer, String, String, Timestamp>, Tuple4<Integer, String, String, Timestamp>>,
        Triggerable<K, VoidNamespace> {

    private static final long serialVersionUID = -5380774605111543454L;

    private static final Logger logger = LoggerFactory.getLogger(IntervalJoinOperator.class);

    private static final String BUFFER = "BUFFER";
    private static final String CLEANUP_TIMER_NAME = "CLEANUP_TIMER";

    private final long lowerBound;
    private final long upperBound;

    private transient MapState<Long, List<Tuple4<Integer, String, String, Timestamp>>> buffer;

    private transient TimestampedCollector<Tuple4<Integer, String, String, Timestamp>> collector;
    private transient ContextImpl context;

    private transient InternalTimerService<VoidNamespace> internalTimerService;

    public IntervalFilterOperator(
            long lowerBound,
            long upperBound,
            boolean lowerBoundInclusive,
            boolean upperBoundInclusive,
            ProcessFunction<Tuple4<Integer, String, String, Timestamp>, Tuple4<Integer, String, String, Timestamp>> udf) {

        super(Preconditions.checkNotNull(udf));

        Preconditions.checkArgument(lowerBound <= upperBound,
                "lowerBound <= upperBound must be fulfilled");

        // Move buffer by +1 / -1 depending on inclusiveness in order not needing
        // to check for inclusiveness later on
        this.lowerBound = (lowerBoundInclusive) ? lowerBound : lowerBound + 1L;
        this.upperBound = (upperBoundInclusive) ? upperBound : upperBound - 1L;

    }

    @Override
    public void open() throws Exception {
        super.open();

        collector = new TimestampedCollector<>(output);
        internalTimerService =
                getInternalTimerService(CLEANUP_TIMER_NAME, VoidNamespaceSerializer.INSTANCE, this);


        SimpleTimerService timerService = new SimpleTimerService(internalTimerService);
        context = new ContextImpl(userFunction, timerService);
    }

    @Override
    public void initializeState(StateInitializationContext context) throws Exception {
        super.initializeState(context);

        TypeInformation<Long> keyInfo = TypeInformation.of(Long.class);
        TypeInformation<List<Tuple4<Integer, String, String, Timestamp>>> valueInfo = TypeInformation.of(new TypeHint<List<Tuple4<Integer, String, String, Timestamp>>>() {
        });
        this.buffer = context.getKeyedStateStore().getMapState(new MapStateDescriptor<>(
                BUFFER,
                keyInfo,
                valueInfo)
        );
    }


    @Override
    public void processElement(StreamRecord<Tuple4<Integer, String, String, Timestamp>> record) throws Exception {
        long timestamp = record.getTimestamp();
        if (timestamp == Long.MIN_VALUE) {
            throw new FlinkException("Long.MIN_VALUE timestamp: Elements used in " +
                    "interval stream filter need to have timestamps meaningful timestamps.");
        }

        if (isLate(timestamp)) {
            return;
        }

        Tuple4<Integer, String, String, Timestamp> recordValue = record.getValue();

        boolean finded = false;
        for (Map.Entry<Long, List<Tuple4<Integer, String, String, Timestamp>>> bucket : buffer.entries()) {

            logger.info("Compare bucket: {}", bucket);
            final Long bucketTimestamp = bucket.getKey();

            if (bucketTimestamp < timestamp + lowerBound ||
                    bucketTimestamp > timestamp + upperBound) {
                logger.info("bucketTimestamp should be skipped. timestamp is {}, bucketTimestamp is {}", timestamp, bucketTimestamp);
                continue;
            }

            collector.setAbsoluteTimestamp(timestamp);
            context.updateTimestamps(timestamp);
            List<Tuple4<Integer, String, String, Timestamp>> bucketes = bucket.getValue();

            for (Tuple4<Integer, String, String, Timestamp> bucketCache : bucketes) {
                if (bucketCache.f2.equals(recordValue.f2)) {
                    finded = true;
                }
            }
            if (!finded) {
                userFunction.processElement(recordValue, context, collector);
            }
        }

        long cleanupTime = (upperBound > 0) ? timestamp + upperBound : timestamp;
        logger.info("Register cleanup timer: {}", cleanupTime);
        internalTimerService.registerEventTimeTimer(VoidNamespace.INSTANCE, cleanupTime);

        addToBuffer(buffer, recordValue, timestamp);
    }

    private boolean isLate(long timestamp) {
        long currentWatermark = internalTimerService.currentWatermark();
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
    public void onEventTime(InternalTimer<K, VoidNamespace> timer) throws Exception {

        long timerTimestamp = timer.getTimestamp();
        logger.info("onEventTime @ {}", timerTimestamp);
        long timestamp = (upperBound <= 0L) ? timerTimestamp : timerTimestamp - upperBound;
        logger.info("Removing from buffer @ {}", timestamp);
        buffer.remove(timestamp);
    }

    @Override
    public void onProcessingTime(InternalTimer<K, VoidNamespace> timer) throws Exception {
        // do nothing.
    }

    private final class ContextImpl extends ProcessFunction<Tuple4<Integer, String, String, Timestamp>, Tuple4<Integer, String, String, Timestamp>>.Context {

        private long currentTimestamp = Long.MIN_VALUE;

        private TimerService timerService;

        private ContextImpl(ProcessFunction<Tuple4<Integer, String, String, Timestamp>, Tuple4<Integer, String, String, Timestamp>> func,
                            TimerService timerService) {
            func.super();
            this.timerService = timerService;
        }

        private void updateTimestamps(long currentTimestamp) {
            this.currentTimestamp = currentTimestamp;
        }


        @Override
        public Long timestamp() {
            return currentTimestamp;
        }

        @Override
        public TimerService timerService() {
            return timerService;
        }

        @Override
        public <X> void output(OutputTag<X> outputTag, X value) {
            Preconditions.checkArgument(outputTag != null, "OutputTag must not be null");
            output.collect(outputTag, new StreamRecord<>(value, timestamp()));
        }
    }

}
