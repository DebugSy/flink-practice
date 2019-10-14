package com.flink.demo.pattern;

import org.junit.Test;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by P0007 on 2019/9/26.
 */
public class PatternTest {

    @Test
    public void testPattern() {
        //KafkaConsumer.topic.topicName.partition -> topicName
        Pattern KAFKA_TOPIC_PATTERN = Pattern.compile("^*KafkaConsumer\\.topic\\.(.*)\\.partition");
        Matcher matcher = KAFKA_TOPIC_PATTERN.matcher("0.Source__shiy_kafka_source.KafkaConsumer.topic.shiy.partition.0.committedOffsets");
        if (matcher.find()){
            String group0 = matcher.group(0);
            String group1 = matcher.group(1);
            System.out.println("group0 -> " + group0);
            System.out.println("group1 -> " + group1);
        }
    }

}
