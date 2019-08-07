package com.flink.demo.cases.case02;

import com.flink.demo.cases.common.datasource.UrlClickDataSource;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.calcite.shaded.com.google.common.base.Strings;
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
 * Created by DebugSy on 2019/8/7.
 * <p>
 * java case 02
 * Flink DataStream 训练 - 与维表join
 * datastream似乎没有这样的api能直接调用，所以使用map替代
 */
public class FlinkDataStreamTraining_join_function {

    private static final Logger logger = LoggerFactory.getLogger(FlinkDataStreamTraining_join_function.class);

    private static Map<String, String> users = new HashMap<>();

    static {
        users.put("用户A", "地址A");
        users.put("用户B", "地址B");
        users.put("用户C", "地址C");
        users.put("用户D", "地址D");
    }

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
//        logger.info("Set timeCharacteristic {}", TimeCharacteristic.EventTime);

        DataStreamSource<Tuple3<String, String, Timestamp>> urlClickSource = env.addSource(new UrlClickDataSource());
        KeyedStream<Tuple3<String, String, Timestamp>, Tuple> keyedStream = urlClickSource.assignTimestampsAndWatermarks(new AscendingTimestampExtractor<Tuple3<String, String, Timestamp>>() {
            @Override
            public long extractAscendingTimestamp(Tuple3<String, String, Timestamp> element) {
                return element.f2.getTime();
            }
        }).keyBy(0);

        DataStream<Row> returns = keyedStream
                .map((MapFunction<Tuple3<String, String, Timestamp>, Row>) value -> {
                    Row row = new Row(value.getArity() + 1);//增加address一列
                    int i;
                    for (i = 0; i < value.getArity(); i++) {
                        row.setField(i, value.getField(i));
                    }
                    String username = value.f0;
                    if (!Strings.isNullOrEmpty(username)) {
                        String address = users.get(username);
                        logger.info("{} # {}", username, address);
                        row.setField(i, address);
                    } else {
                        row.setField(i, "其他");
                    }
                    return row;
                });

        returns.printToErr();

        env.execute("Flink Datastream Training join static data");
    }

}

