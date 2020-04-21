package com.flink.demo.cases.case26;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.types.Row;

import java.util.List;

/**
 * @author P0007
 * @version 1.0
 * @date 2020/4/21 14:16
 * <p>
 * 最终状态触发窗口计算
 */
@Slf4j
public class FinalStateTriggerWindow {

    private DataStream<Row> inputStream;

    private RowTypeInfo rowTypeInfo;

    public FinalStateTriggerWindow(DataStream<Row> inputStream, RowTypeInfo rowTypeInfo) {
        this.inputStream = inputStream;
        this.rowTypeInfo = rowTypeInfo;
    }

    public DataStream<Row> process(int keyColIndex,
                                   int finalStateColumnIndex,
                                   List<String> finalStateValues,
                                   long waitTime,
                                   long lateness) {
        KeyedStream<Row, Tuple> keyedStream = inputStream.keyBy(keyColIndex);
        SingleOutputStreamOperator<Row> finalStateResult = keyedStream.timeWindow(Time.seconds(waitTime))
                .allowedLateness(Time.seconds(lateness))
                .trigger(new FinalStateTrigger(finalStateColumnIndex, finalStateValues))
                .process(new FinalStateAggregate(rowTypeInfo))
                .name("FinalStateTriggerWindow")
                .uid("FinalStateTriggerWindow");
        return finalStateResult;
    }





}
