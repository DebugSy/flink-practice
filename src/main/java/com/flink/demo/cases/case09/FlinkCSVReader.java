package com.flink.demo.cases.case09;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.formats.csv.CsvRowDeserializationSchema;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.table.api.Types;
import org.apache.flink.types.Row;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import java.util.Properties;

/**
 * Created by P0007 on 2019/8/20.
 *
 * 用flink自带的反序列化器 CsvRowDeserializationSchema 读取kafka数据
 */
public class FlinkCSVReader {

    private static TypeInformation userClickTypeInfo = Types.ROW(
            new String[]{"userId","username","url","clickTime"},
            new TypeInformation[]{
                    Types.INT(),
                    Types.STRING(),
                    Types.STRING(),
                    Types.SQL_TIMESTAMP()
            });

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        CsvRowDeserializationSchema deserializationSchema = new CsvRowDeserializationSchema.Builder(userClickTypeInfo)
                .setQuoteCharacter('\"')
                .setEscapeCharacter('\\')
                .setNullLiteral("")
                .setFieldDelimiter(',')
                .setArrayElementDelimiter(";")
                .setAllowComments(true)
                .build();

        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");

        FlinkKafkaConsumer010<Row> kafkaConsumer010 = new FlinkKafkaConsumer010<>("kafka_sink", deserializationSchema, props);
        DataStreamSource<Row> streamSource = env.addSource(kafkaConsumer010);
        streamSource.printToErr();

        env.execute("Flink Kafka Read");
    }

}
