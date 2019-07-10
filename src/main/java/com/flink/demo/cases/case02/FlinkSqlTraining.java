package com.flink.demo.cases.case02;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Timestamp;
import java.util.Random;

/**
 * Created by DebugSy on 2019/7/10.
 */
public class FlinkSqlTraining {

    private static final Logger logger = LoggerFactory.getLogger(FlinkSqlTraining.class);

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        StreamTableEnvironment tableEnv = TableEnvironment.getTableEnvironment(env);

        DataStreamSource<Tuple3<String, String, Timestamp>> sourceStream = env.addSource(new DataSource());
        sourceStream.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<Tuple3<String, String, Timestamp>>(Time.seconds(5)) {
            @Override
            public long extractTimestamp(Tuple3<String, String, Timestamp> element) {
                return element.f2.getTime();
            }
        });

        tableEnv.registerDataStream("clicks", sourceStream, "username,url,clickTime.rowtime");

        Table sqlQuery = tableEnv.sqlQuery("select " +
                "username, " +
                "count(*), " +
                "TUMBLE_START(clickTime, INTERVAL '5' SECOND), " +
                "TUMBLE_END(clickTime, INTERVAL '5' SECOND) " +
                "from clicks " +
                "group by username, " +
                "TUMBLE(clickTime, INTERVAL '5' SECOND)");

        DataStream<Tuple2<Boolean, Row>> sinkStream = tableEnv.toRetractStream(sqlQuery, Row.class);
        sinkStream.addSink(new SinkFunction<Tuple2<Boolean, Row>>() {
            @Override
            public void invoke(Tuple2<Boolean, Row> value, Context context) throws Exception {
                logger.error("print retract:{} -> {}", value.f0, value.f1);
            }
        }).name("Print to Std.Error");


        env.execute("Flink SQL Training");
    }

    static class DataSource extends RichSourceFunction<Tuple3<String, String, Timestamp>> {

        private volatile boolean running = true;

        @Override
        public void run(SourceContext<Tuple3<String, String, Timestamp>> ctx) throws Exception {
            Random random = new Random(System.currentTimeMillis());
            while (running) {
                int indexOfThisSubtask = getRuntimeContext().getIndexOfThisSubtask();
                logger.info("The index of the parallel subtask is {}", indexOfThisSubtask);
                Thread.sleep( (indexOfThisSubtask + 1 ) * 1000 + 500 );
                String username = "用户" + (char)('A' + random.nextInt(5));
                Timestamp clickTime = new Timestamp(System.currentTimeMillis());
                String url = "http://127.0.0.1/api/" + (char)('H' + random.nextInt(4));
                ctx.collect(new Tuple3<String, String, Timestamp>(username, url, clickTime));
            }
        }

        @Override
        public void cancel() {
            running = false;
        }

    }

}
