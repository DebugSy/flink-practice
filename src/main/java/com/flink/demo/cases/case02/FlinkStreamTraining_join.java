package com.flink.demo.cases.case02;

import com.flink.demo.cases.common.datasource.UrlClickRowDataSource;
import com.flink.demo.cases.common.datasource.UserRowDataSource;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.scala.KeySelectorWithType;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.types.Row;

import java.sql.Timestamp;

/**
 * Created by P0007 on 2019/10/12.
 */
public class FlinkStreamTraining_join {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        SingleOutputStreamOperator<Row> clickStream = env.addSource(new UrlClickRowDataSource())
                .returns(UrlClickRowDataSource.USER_CLICK_TYPEINFO);
        SingleOutputStreamOperator<Row> clickStreamAndWatermarks = clickStream.assignTimestampsAndWatermarks(new AscendingTimestampExtractor<Row>() {
            @Override
            public long extractAscendingTimestamp(Row element) {
                return Timestamp.valueOf(element.getField(3).toString()).getTime();
            }
        });

        SingleOutputStreamOperator<Row> userStream = env.addSource(new UserRowDataSource())
                .returns(UserRowDataSource.USER_TYPEINFO);
        SingleOutputStreamOperator<Row> userStreamAndWatermarks = userStream.assignTimestampsAndWatermarks(new AscendingTimestampExtractor<Row>() {
            @Override
            public long extractAscendingTimestamp(Row element) {
                return Timestamp.valueOf(element.getField(3).toString()).getTime();
            }
        });

        int arity = clickStream.getType().getArity() + userStream.getType().getArity();
        final Row result = new Row(arity);

        DataStream<Row> dataStream = clickStreamAndWatermarks.join(userStreamAndWatermarks)
                .where(new KeySelector<Row, Object>() {
                    @Override
                    public Object getKey(Row value) throws Exception {
                        return value.getField(0);
                    }
                })
                .equalTo(new KeySelector<Row, Object>() {
                    @Override
                    public Object getKey(Row value) throws Exception {
                        return value.getField(0);
                    }
                })
                .window(TumblingEventTimeWindows.of(Time.seconds(5)))
                .apply(new JoinFunction<Row, Row, Row>() {
                    @Override
                    public Row join(Row first, Row second) throws Exception {
                        for (int i = 0; i < first.getArity(); i++) {
                            result.setField(i, first.getField(i));
                        }
                        for (int i = first.getArity(); i < first.getArity() + second.getArity(); i++) {
                            result.setField(i, second.getField(i - first.getArity()));
                        }
                        return result;
                    }
                });

        dataStream.printToErr();

        env.execute("Flink Stream Join Training");
    }

}
