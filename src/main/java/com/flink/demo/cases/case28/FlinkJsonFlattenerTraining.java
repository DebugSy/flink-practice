package com.flink.demo.cases.case28;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.table.api.Types;
import org.apache.flink.types.Row;
import com.github.wnameless.json.flattener.JsonFlattener;

import javax.annotation.Nullable;
import java.sql.Timestamp;
import java.util.Map;
import java.util.Properties;

/**
 * @author P0007
 * @version 1.0
 * @date 2020/4/28 18:32
 */
@Slf4j
public class FlinkJsonFlattenerTraining {

    private static ObjectMapper MAPPER = new ObjectMapper();

    public static TypeInformation USER_CLICK_TYPEINFO = Types.ROW(
            new String[]{"userId", "username", "url", "clickTime", "random", "uuid", "date.data_col", "date.time_col", "final_state"},
            new TypeInformation[]{
                    Types.STRING(),
                    Types.STRING(),
                    Types.STRING(),
                    Types.STRING(),
                    Types.STRING(),
                    Types.STRING(),
                    Types.STRING(),
                    Types.STRING(),
                    Types.STRING()
            });


    public static void main(String[] args) throws Exception {
        Configuration configuration = new Configuration();
        StreamExecutionEnvironment env = StreamExecutionEnvironment
                .createLocalEnvironmentWithWebUI(configuration);
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        final RowTypeInfo rowTypeInfo = (RowTypeInfo) USER_CLICK_TYPEINFO;
        Row row = new Row(rowTypeInfo.getArity());

        Properties props = new Properties();
        props.setProperty("bootstrap.servers", "192.168.2.170:9092");
        props.setProperty("group.id", "test");
        FlinkKafkaConsumer010<String> kafkaConsumer010 =
                new FlinkKafkaConsumer010<>("local_test_json", new SimpleStringSchema(), props);
        kafkaConsumer010.setStartFromLatest();
        SingleOutputStreamOperator<Row> urlClickSource = env.addSource(kafkaConsumer010)
                .map(new RichMapFunction<String, Row>() {
                    @Override
                    public Row map(String value) throws Exception {
                        log.info("read value {}", value);
                        String flatten = JsonFlattener.flatten(value);
                        Map<String, Object> values = MAPPER.readValue(flatten, Map.class);
                        log.info("flatten value {}", flatten);
                        String[] fieldNames = rowTypeInfo.getFieldNames();
                        for (int i = 0; i < fieldNames.length; i++) {
                            String fieldName = fieldNames[i];
                            row.setField(i, values.get(fieldName));
                        }
                        return row;
                    }
                })
                .returns(rowTypeInfo)
                .name("url click stream source");

        SingleOutputStreamOperator<Row> streamOperator = urlClickSource.assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks<Row>() {

            private long currentTimestamp = Long.MIN_VALUE;

            @Nullable
            @Override
            public Watermark getCurrentWatermark() {
                long watermark = currentTimestamp == Long.MIN_VALUE ? Long.MIN_VALUE : currentTimestamp;
                log.debug("current watermark is {}", watermark);
                return new Watermark(watermark);
            }

            @Override
            public long extractTimestamp(Row element, long previousElementTimestamp) {
                long newTimestamp = 0;
                newTimestamp = Timestamp.valueOf(element.getField(3).toString()).getTime();
                if (newTimestamp > this.currentTimestamp) {
                    this.currentTimestamp = newTimestamp;
                }
                log.debug("extractTimestamp {}", this.currentTimestamp);
                return this.currentTimestamp;
            }
        });
        streamOperator.printToErr();

        env.execute("Incremental Window Aggregation with ReduceFunction");

    }

}
