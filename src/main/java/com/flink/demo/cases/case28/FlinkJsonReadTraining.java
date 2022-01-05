package com.flink.demo.cases.case28;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.formats.json.JsonRowDeserializationSchema;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.table.api.Types;
import org.apache.flink.types.Row;

import javax.annotation.Nullable;
import java.sql.Timestamp;
import java.util.Properties;

/**
 * @author P0007
 * @version 1.0
 * @date 2020/4/28 18:32
 */
@Slf4j
public class FlinkJsonReadTraining {

    /*
    * json数据: {"userId":"66","username":"userI8fcf2bbd-7564-401a-95c0-8eee091021ce","url":"http://127.0.0.1/api/K,2020-04-28","clickTime":"2020-04-28 17:11:17.053","random":"95","uuid":"0e221c9a-4395-4885-a98f-93d3af9a49c3", "date":{"data_col":"20200428","time_col":"171117"},"final_state":"MIDDLE_1"}
    * */

    private static final String jsonSchema2 = "{ " +
            "    \"type\": \"object\",     " +
            "    \"properties\": {      " +
            "        \"userId\": {\"type\" : \"string\"}," +
            "        \"username\": {\"type\" : \"string\"}," +
            "        \"url\": {\"type\" : \"string\"}," +
            "        \"clickTime\": {\"type\" : \"string\"}," +
            "        \"random\": {\"type\" : \"string\"}," +
            "        \"uuid\": {\"type\" : \"string\"}," +
            "        \"date\": {" +
            "            \"type\" : \"object\"," +
            "            \"properties\" : {" +
            "            \"data_col\": {\"type\" : \"string\"}," +
            "            \"time_col\": {\"type\" : \"string\"}" +
            "            }" +
            "           }," +
            "        \"final_state\": {\"type\" : \"string\"}" +
            "    }" +
            "}";

    public static TypeInformation USER_CLICK_TYPEINFO = Types.ROW(
            new String[]{"userId", "username", "url", "clickTime", "random", "uuid", "data", "final_state"},
            new TypeInformation[]{
                    Types.STRING(),
                    Types.STRING(),
                    Types.STRING(),
                    Types.STRING(),
                    Types.STRING(),
                    Types.STRING(),
                    Types.ROW(
                            new String[]{"data_col", "time_col"},
                            new TypeInformation[]{
                                    Types.STRING(),
                                    Types.STRING()
                            }),
                    Types.STRING()
            });

    public static void main(String[] args) throws Exception {
        Configuration configuration = new Configuration();
        StreamExecutionEnvironment env = StreamExecutionEnvironment
                .createLocalEnvironmentWithWebUI(configuration);
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        final RowTypeInfo rowTypeInfo = (RowTypeInfo) USER_CLICK_TYPEINFO;

        Properties props = new Properties();
        props.setProperty("bootstrap.servers", "192.168.2.170:9092");
        props.setProperty("group.id", "test");
        FlinkKafkaConsumer010<Row> kafkaConsumer010 =
                new FlinkKafkaConsumer010<>("local_test_json", new JsonRowDeserializationSchema(jsonSchema2), props);
        kafkaConsumer010.setStartFromLatest();
        SingleOutputStreamOperator<Row> urlClickSource = env.addSource(kafkaConsumer010)
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
