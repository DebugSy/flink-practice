package com.flink.demo.cases.case21;

import org.apache.flink.api.common.io.FileInputFormat;
import org.apache.flink.api.java.io.TextInputFormat;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.FileProcessingMode;

/**
 * Created by P0007 on 2020/1/20.
 */
public class HDFSSource {

    private static String path = "/tmp/data/flink/read_file/";

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);

        FileInputFormat<String> inputFormat = new TextInputFormat(null);
        DataStreamSource<String> streamSource = env.readFile(
                inputFormat,
                path,
                FileProcessingMode.PROCESS_ONCE, 1000);

        streamSource.printToErr();

        env.execute("Read HDFS File");

    }

}
