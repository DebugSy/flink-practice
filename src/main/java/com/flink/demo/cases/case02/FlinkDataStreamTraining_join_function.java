package com.flink.demo.cases.case02;

import com.flink.demo.cases.common.datasource.UrlClickDataSource;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.calcite.shaded.com.google.common.base.Strings;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.dropwizard.metrics.DropwizardMeterWrapper;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Meter;
import org.apache.flink.metrics.MeterView;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Timestamp;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by DebugSy on 2019/8/7.
 * <p>
 * java case 02
 * Flink DataStream 训练 - 与维表join
 * datastream似乎没有这样的api能直接调用，所以使用map替代
 */
public class FlinkDataStreamTraining_join_function {

    private static final Logger logger = LoggerFactory.getLogger(FlinkDataStreamTraining_join_function.class);

    private static Map<String, String> users = new HashMap<>();

    static {
        users.put("用户A", "地址A");
        users.put("用户B", "地址B");
        users.put("用户C", "地址C");
        users.put("用户D", "地址D");
    }

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
//        logger.info("Set timeCharacteristic {}", TimeCharacteristic.EventTime);

        DataStreamSource<Tuple3<String, String, Timestamp>> urlClickSource = env.addSource(new UrlClickDataSource());
        KeyedStream<Tuple3<String, String, Timestamp>, Tuple> keyedStream = urlClickSource.assignTimestampsAndWatermarks(new AscendingTimestampExtractor<Tuple3<String, String, Timestamp>>() {
            @Override
            public long extractAscendingTimestamp(Tuple3<String, String, Timestamp> element) {
                return element.f2.getTime();
            }
        }).keyBy(0);

        DataStream<Row> returns = keyedStream
                .map(new LookupFunction(users))
                .setParallelism(1)
                .name("lookup");

        returns.printToErr();

        env.execute("Flink Datastream Training join static data");
    }

}

class LookupFunction extends RichMapFunction<Tuple3<String, String, Timestamp>, Row> {

    private Map<String, String> users;

    private transient Meter meter;

    private transient Counter counter;

    private transient DropwizardMeterWrapper meterWrapper;

    public LookupFunction(Map<String, String> users) {
        this.users = users;
    }

    @Override
    public void open(Configuration parameters) throws Exception {

        MetricGroup customGroup = getRuntimeContext()
                .getMetricGroup()
                .addGroup("custom_group");

        counter = customGroup.counter("hit_count");

        meter = customGroup.meter("hit", new MeterView(counter, 5));

        MetricGroup dropwizardGroup = getRuntimeContext()
                .getMetricGroup()
                .addGroup("dropwizard_group");
        com.codahale.metrics.Meter dropwizardMeter = new com.codahale.metrics.Meter();
        meterWrapper = dropwizardGroup.meter("hit", new DropwizardMeterWrapper(dropwizardMeter));
    }

    @Override
    public Row map(Tuple3<String, String, Timestamp> value) throws Exception {
        Row row = new Row(value.getArity() + 1);//增加address一列
        int i;
        for (i = 0; i < value.getArity(); i++) {
            row.setField(i, value.getField(i));
        }
        String username = value.getField(0);
        String address = users.get(username);
        if (!Strings.isNullOrEmpty(address)) {
            counter.inc();
            meter.markEvent();
            meterWrapper.markEvent();
            row.setField(i, address);
        } else {
            row.setField(i, "其他");
        }
        return row;
    }
}

