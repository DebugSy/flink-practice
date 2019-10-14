package com.flink.demo.cases.case06;

import com.flink.demo.cases.common.datasource.UrlClickRowDataSource;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import static com.flink.demo.cases.common.datasource.UrlClickRowDataSource.USER_CLICK_TYPEINFO;

/**
 * Created by P0007 on 2019/10/9.
 */
public class FlinkDatastreamAgg {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);
        env.setParallelism(1);

        DataStream<Row> source = env
                .addSource(new UrlClickRowDataSource())
                .returns(USER_CLICK_TYPEINFO);




        KeyedStream keyedStream = source.keyBy(new KeySelector<Row, List<String>>() {
            @Override
            public List<String> getKey(Row value) throws Exception {
                return Arrays.asList(value.getField(0).toString());
            }
        });

        DataStream<Row> aggregateTraining = keyedStream
//                .window(SlidingEventTimeWindows.of(Time.seconds(10), Time.seconds(5)))
                .window(SlidingProcessingTimeWindows.of(Time.seconds(10), Time.seconds(5)))
                .aggregate(new FlinkAggregateFunction(), new WindowFunction<Row, Row, List<String>, TimeWindow>() {
                    @Override
                    public void apply(List<String> key, TimeWindow window, Iterable<Row> input, Collector<Row> out) throws Exception {
                        Row row = new Row(2);
                        row.setField(0, key);
                        Iterator<Row> iterator = input.iterator();
                        int i = 1;
                        while (iterator.hasNext()) {
                            Row next = iterator.next();
                            row.setField(i, next.getField(i - 1));
                            i++;
                        }
                        out.collect(row);
                    }
                })
                .name("Flink Aggregate Training");

        aggregateTraining.printToErr();

        env.execute("Flink Aggregate Training");

    }

}
