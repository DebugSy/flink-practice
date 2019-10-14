package com.flink.demo.cases.case17;

import com.codahale.metrics.Histogram;
import com.codahale.metrics.SlidingWindowReservoir;
import com.flink.demo.cases.common.datasource.UrlClickDataSource;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.calcite.shaded.com.google.common.base.Strings;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.dropwizard.metrics.DropwizardHistogramWrapper;
import org.apache.flink.dropwizard.metrics.DropwizardMeterWrapper;
import org.apache.flink.metrics.*;
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
 * Created by DebugSy on 2019/10/11.
 * <p>
 * java case 17
 * Flink Metrics 训练 - 与维表join
 * datastream似乎没有这样的api能直接调用，所以使用map替代
 */
public class FlinkMetricsTraining {

    private static final Logger logger = LoggerFactory.getLogger(FlinkMetricsTraining.class);

    private static Map<String, String> users = new HashMap<>();

    static {
        users.put("用户A", "地址A");
        users.put("用户B", "地址B");
        users.put("用户C", "地址C");
        users.put("用户D", "地址D");
    }

    public static void main(String[] args) throws Exception {
        Configuration configuration = new Configuration();

        //influx db
        configuration.setString("metrics.reporter.influxdb.class", "org.apache.flink.metrics.influxdb.InfluxdbReporter");
        configuration.setString("metrics.reporter.influxdb.host", "localhost");
        configuration.setString("metrics.reporter.influxdb.port", "8086");
        configuration.setString("metrics.reporter.influxdb.db", "flink_metrics");
        configuration.setString("metrics.reporter.influxdb.username", "admin");
        configuration.setString("metrics.reporter.influxdb.password", "admin");

        //slf4j
        configuration.setString("metrics.reporter.slf4j.class", "org.apache.flink.metrics.slf4j.Slf4jReporter");
        configuration.setString("metrics.reporter.slf4j.interval", "30 SECONDS");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(configuration);
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        DataStreamSource<Tuple4<Integer, String, String, Timestamp>> urlClickSource = env.addSource(new UrlClickDataSource());
        KeyedStream<Tuple4<Integer, String, String, Timestamp>, Tuple> keyedStream = urlClickSource.assignTimestampsAndWatermarks(
                new AscendingTimestampExtractor<Tuple4<Integer, String, String, Timestamp>>() {
                    @Override
                    public long extractAscendingTimestamp(Tuple4<Integer, String, String, Timestamp> element) {
                        return element.f3.getTime();
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

class LookupFunction extends RichMapFunction<Tuple4<Integer, String, String, Timestamp>, Row> {

    private Map<String, String> users;

    //flink自带MeterView，统计60s内每秒lookup次数
    private transient Meter meter;

    //统计lookup总次数
    private transient Counter counter;

    //缓存命中率-总计数器
    private transient DropwizardMeterWrapper totalMeterWrapper;

    //缓存命中率-命中计数器
    private transient DropwizardMeterWrapper hitMeterWrapper;

    //统计1000次lookup，耗时分布情况
    private transient DropwizardHistogramWrapper histogram;

    //使用gauge统计每次耗时
    private transient long procTime;

    public LookupFunction(Map<String, String> users) {
        this.users = users;
    }

    @Override
    public void open(Configuration parameters) throws Exception {

        MetricGroup customGroup = getRuntimeContext()
                .getMetricGroup()
                .addGroup("custom_group");

        counter = customGroup.counter("hit_count_cp");

        Histogram _histogram = new Histogram(new SlidingWindowReservoir(1000));
        this.histogram = customGroup.histogram("process_time", new DropwizardHistogramWrapper(_histogram));

        customGroup.gauge("processTime", (Gauge<Long>) () -> procTime);

        meter = customGroup.meter("hit", new MeterView(customGroup.counter("hit_count"), 60));

        MetricGroup dropwizardGroup = getRuntimeContext()
                .getMetricGroup()
                .addGroup("dropwizard_group");
        com.codahale.metrics.Meter dropwizardMeter = new com.codahale.metrics.Meter();
        totalMeterWrapper = dropwizardGroup.meter("lookup_total", new DropwizardMeterWrapper(dropwizardMeter));

        com.codahale.metrics.Meter hitMeter = new com.codahale.metrics.Meter();
        hitMeterWrapper = dropwizardGroup.meter("lookup_hit", new DropwizardMeterWrapper(hitMeter));


        CacheHitRatio cacheHitRatio = new CacheHitRatio(hitMeter, dropwizardMeter);
        DropwizardRatioWrapper ratioWrapper = new DropwizardRatioWrapper(cacheHitRatio);
        customGroup.gauge("lookup_hit_ratio", ratioWrapper);
    }

    @Override
    public Row map(Tuple4<Integer, String, String, Timestamp> value) throws Exception {
        long startTime = System.nanoTime();
        Row row = new Row(value.getArity() + 1);//增加address一列
        int i;
        for (i = 0; i < value.getArity(); i++) {
            row.setField(i, value.getField(i));
        }
        String username = value.getField(0);
        String address = users.get(username);
        if (!Strings.isNullOrEmpty(address)) {
            hitMeterWrapper.markEvent();
            row.setField(i, address);
        } else {
            row.setField(i, "其他");
        }
        counter.inc();
        meter.markEvent();
        totalMeterWrapper.markEvent();
        long endTime = System.nanoTime();
        procTime = endTime - startTime;
        this.histogram.update(procTime);
        return row;
    }
}

