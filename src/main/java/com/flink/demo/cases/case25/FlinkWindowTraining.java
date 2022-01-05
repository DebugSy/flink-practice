package com.flink.demo.cases.case25;

import com.flink.demo.cases.common.datasource.UrlClickRowDataSource;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;

@Slf4j
public class FlinkWindowTraining {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        SingleOutputStreamOperator<Row> urlClickSource = env.addSource(new UrlClickRowDataSource())
                .returns(UrlClickRowDataSource.USER_CLICK_TYPEINFO)
                .name("url click stream source");

        SingleOutputStreamOperator<Row> streamOperator = urlClickSource.assignTimestampsAndWatermarks(new AscendingTimestampExtractor<Row>() {
            @Override
            public long extractAscendingTimestamp(Row element) {
                return Timestamp.valueOf(element.getField(3).toString()).getTime();
            }
        });

        KeyedStream<Row, Tuple> keyedStream = streamOperator.keyBy(1);

        SingleOutputStreamOperator<Row> aggregate = keyedStream.timeWindow(Time.seconds(600))
                .trigger(new Trigger<Row, TimeWindow>() {
                    @Override
                    public TriggerResult onElement(Row element, long timestamp, TimeWindow window, TriggerContext ctx) throws Exception {
                        log.info("OnElement Event, element is {}, timestamp is {}, watermark is {}, window is {}",
                                element, timestamp, ctx.getCurrentWatermark(), window);
                        if (ctx.getCurrentWatermark() <= window.maxTimestamp()) {
                            log.info("OnElement Event, TriggerResult is CONTINUE");
                            return TriggerResult.CONTINUE;
                        } else {
                            log.info("OnElement Event, TriggerResult is FIRE");
                            ctx.registerEventTimeTimer(window.maxTimestamp());
                            log.info("----");
                            return TriggerResult.FIRE;
                        }
                    }

                    @Override
                    public TriggerResult onProcessingTime(long time, TimeWindow window, TriggerContext ctx) throws Exception {
                        log.info("onProcessingTime Event, time is {}, window is {}", window);
                        return TriggerResult.CONTINUE;
                    }

                    @Override
                    public TriggerResult onEventTime(long time, TimeWindow window, TriggerContext ctx) throws Exception {
                        log.info("onEventTime Event, time is {}, watermark is {}, window max timestamp is {}, window is {}",
                                time, ctx.getCurrentWatermark(), window.maxTimestamp(), window);
                        return time == window.maxTimestamp() ? TriggerResult.FIRE : TriggerResult.CONTINUE;
                    }

                    @Override
                    public void clear(TimeWindow window, TriggerContext ctx) throws Exception {
                        log.info("OnClear Event, watermark is {}", ctx.getCurrentWatermark());
                        ctx.deleteEventTimeTimer(window.maxTimestamp());
                    }
                })
                .aggregate(new AggregateFunction<Row, Long, Long>() {

                               @Override
                               public Long createAccumulator() {
                                   return 0L;
                               }

                               @Override
                               public Long add(Row value, Long accumulator) {
                                   return ++accumulator;
                               }

                               @Override
                               public Long getResult(Long accumulator) {
                                   return accumulator;
                               }

                               @Override
                               public Long merge(Long a, Long b) {
                                   return a + b;
                               }
                           },
                        new ProcessWindowFunction<Long, Row, Tuple, TimeWindow>() {

                            @Override
                            public void process(Tuple tuple,
                                                Context context,
                                                Iterable<Long> elements,
                                                Collector<Row> out) throws Exception {
                                Long count = elements.iterator().next();
                                Row row = new Row(3);
                                row.setField(0, context.window().getStart());
                                row.setField(1, tuple.getField(0));
                                row.setField(2, count);
                                out.collect(row);
                            }
                        });

        aggregate.printToErr();


        env.execute("Incremental Window Aggregation with ReduceFunction");

    }

}
