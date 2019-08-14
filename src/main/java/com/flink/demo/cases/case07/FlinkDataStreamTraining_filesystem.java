package com.flink.demo.cases.case07;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.io.TextInputFormat;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.FileProcessingMode;
import org.apache.flink.streaming.connectors.fs.bucketing.BucketingSink;
import org.apache.flink.streaming.connectors.fs.bucketing.DateTimeBucketer;

/**
 * java case 07
 * Flink DataStream 训练 - FileSystem读写
 */
public class FlinkDataStreamTraining_filesystem {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.enableCheckpointing(1000 * 10);

        TextInputFormat textInputFormat = new TextInputFormat(null);

        DataStreamSource<String> streamSource = env.readFile(
                textInputFormat,
                "/Users/shiy/tmp/flink/data/filesource/",
                FileProcessingMode.PROCESS_CONTINUOUSLY,
                1000 * 5
                );

        streamSource.printToErr();

        BucketingSink<String> bucketingSink = new BucketingSink<>("file:///Users/shiy/tmp/flink/sink/");
        bucketingSink.setBatchSize(10);
        bucketingSink.setBatchRolloverInterval(1000 * 5);

        streamSource.addSink(bucketingSink);

        env.execute();

    }

}
