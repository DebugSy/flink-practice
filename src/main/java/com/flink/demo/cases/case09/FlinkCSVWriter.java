package com.flink.demo.cases.case09;

import com.flink.demo.cases.common.datasource.UrlClickRowDataSource;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.serialization.TypeInformationSerializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.formats.csv.CsvRowSerializationSchema;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer010;
import org.apache.flink.table.api.Types;
import org.apache.flink.types.Row;
import org.apache.kafka.clients.producer.ProducerConfig;

import java.util.Properties;

/**
 * Created by P0007 on 2019/9/3.
 *
 * flink csv/json 格式测试
 *
 * 用flink自带的序列化器 SerializationSchema 向kafka写入数据
 */
public class FlinkCSVWriter {

    private static TypeInformation userClickTypeInfo = Types.ROW(
            new String[]{"userId", "username", "url", "clickTime"},
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

        DataStream<Row> sourceStream = env
                .addSource(new UrlClickRowDataSource())
                .returns(userClickTypeInfo);
        TypeInformation<Row> rowTypeInfo = sourceStream.getType();
        SerializationSchema serializationSchema = new CsvRowSerializationSchema.Builder(rowTypeInfo)
                .setQuoteCharacter('\"')
                .setEscapeCharacter('\\')
                .setArrayElementDelimiter(";")
                .setLineDelimiter("\r\n")
                .setNullLiteral("")
                .setFieldDelimiter(',')
                .build();

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");

        TypeInformationSerializationSchema typeInformationSerializationSchema = new TypeInformationSerializationSchema(rowTypeInfo, new ExecutionConfig());

        FlinkKafkaProducer010<Row> producer010 = new FlinkKafkaProducer010<Row>("kafka_sink", typeInformationSerializationSchema, props);
        producer010.setWriteTimestampToKafka(true);
        sourceStream.addSink(producer010);

//        BucketingSink<Row> rowBucketingSink = new BucketingSink<Row>("/tmp/rtc-streaming/hdfs_sink")
//                .setBatchSize(1000)
//                .setBatchRolloverInterval(1000 * 5)
//                .setWriter(new StringWriter())
//                .setBucketer(new DateTimeBucketer("yyyy-MM-dd--HHmm"));
//        sourceStream.addSink(rowBucketingSink);


        env.setParallelism(1);
        env.execute("Flink Format Training");

    }

}
