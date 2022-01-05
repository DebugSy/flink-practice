package com.flink.demo.cases.case03;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.jobgraph.SavepointConfigOptions;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.Properties;

public class FlinkKafkaConsumerTraining {

    public static void main(String[] args) throws Exception {
        Configuration configuration = new Configuration();
        configuration.setInteger("state.checkpoints.num-retained", 10);
        configuration.setString(SavepointConfigOptions.SAVEPOINT_PATH,
                "file:///tmp/shiy/flink/checkpoints/checkpoint-2021-11-30/270577923b277b8ba2891f99716be560/chk-3");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(configuration);
        env.enableCheckpointing(1000 * 5, CheckpointingMode.EXACTLY_ONCE);
        env.setStateBackend(new HashMapStateBackend());
        env.getCheckpointConfig().setCheckpointStorage("file:///tmp/shiy/flink/checkpoints/checkpoint-2021-11-30");

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "192.168.1.82:9094");
        properties.setProperty("group.id", "xxxx");
        FlinkKafkaConsumer<String> kafkaConsumer = new FlinkKafkaConsumer<String>(
                "shiy.flink.url.click", new SimpleStringSchema(), properties);

        env.addSource(kafkaConsumer)
                .printToErr();

        env.execute("Flink Kafka Consumer Training");

    }

}
