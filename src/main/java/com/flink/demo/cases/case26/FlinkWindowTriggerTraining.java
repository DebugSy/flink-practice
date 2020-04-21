package com.flink.demo.cases.case26;

import com.flink.demo.cases.common.datasource.UrlClickFinalStateRowDataSource;
import com.flink.demo.cases.common.utils.ClassUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.types.Row;

import java.sql.Timestamp;
import java.util.Arrays;
import java.util.Properties;

@Slf4j
public class FlinkWindowTriggerTraining {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        final RowTypeInfo rowTypeInfo = (RowTypeInfo) UrlClickFinalStateRowDataSource.USER_CLICK_TYPEINFO;

        Properties props = new Properties();
        props.setProperty("bootstrap.servers", "192.168.2.170:9092");
        props.setProperty("group.id", "test");
        FlinkKafkaConsumer010<String> kafkaConsumer010 =
                new FlinkKafkaConsumer010<>("local_test", new SimpleStringSchema(), props);
        SingleOutputStreamOperator<Row> urlClickSource = env.addSource(kafkaConsumer010)
                .map(new RichMapFunction<String, Row>() {

                    Row row = new Row(rowTypeInfo.getArity());

                    @Override
                    public Row map(String value) throws Exception {
                        String[] split = value.split(",", -1);
                        for (int i = 0; i < split.length; i++) {
                            String valueStr = split[i];
                            Object convert = ClassUtil.convert(valueStr, rowTypeInfo.getTypeAt(i).toString());
                            row.setField(i, convert);
                        }
                        log.info("consume message {}", row);
                        return row;
                    }
                })
                .returns(rowTypeInfo)
                .name("url click stream source");

        SingleOutputStreamOperator<Row> streamOperator = urlClickSource.assignTimestampsAndWatermarks(new AscendingTimestampExtractor<Row>() {
            @Override
            public long extractAscendingTimestamp(Row element) {
                return Timestamp.valueOf(element.getField(3).toString()).getTime();
            }
        });

        FinalStateTriggerWindow finalStateTriggerWindow = new FinalStateTriggerWindow(
                streamOperator, (RowTypeInfo) rowTypeInfo);
        DataStream<Row> mergedStream = finalStateTriggerWindow.process(1,
                8,
                Arrays.asList("FinalState1", "FinalState2", "FinalState3"),
                10,
                5);
        mergedStream.printToErr();


        env.execute("Incremental Window Aggregation with ReduceFunction");

    }

}
