package com.flink.demo.cases.case10;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimerService;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Created by P0007 on 2020/3/16.
 * 缓存前面的数据，等待新到的数据与它合并，到达触发时间后往后发送
 *
 * Flink SQL 训练 - 在source 处缓存一段时间的数据
 * 实际场景：中移金科那边kafka中的数据一个流水号会有两条数据过来，两条数据是有间隔的，需要将两条数据合并
 */
@Slf4j
public class CacheStreamFunction extends KeyedProcessFunction<Tuple, Row, Row> {

    private MapState<Object, Long> keyMapState;

    private MapState<Long, List<Row>> mapState;

    private String keyColumn;

    private RowTypeInfo rowTypeInfo;

    private int keyColumnIndex;

    public CacheStreamFunction(String keyColumn, RowTypeInfo rowTypeInfo) {
        this.keyColumn = keyColumn;
        this.rowTypeInfo = rowTypeInfo;
        this.keyColumnIndex = rowTypeInfo.getFieldIndex(keyColumn);
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        TypeInformation<Long> keyInfo = TypeInformation.of(Long.class);
        TypeInformation<List<Row>> valueInfo = TypeInformation.of(new TypeHint<List<Row>>() {
        });
        MapStateDescriptor stateDescriptor = new MapStateDescriptor("cache", keyInfo, valueInfo);
        this.mapState = getRuntimeContext().getMapState(stateDescriptor);

        TypeInformation<Object> keyMapKeyInfo = TypeInformation.of(Object.class);
        TypeInformation<Long> KeyMapvalueInfo = TypeInformation.of(Long.class);
        MapStateDescriptor keyMapDescriptor = new MapStateDescriptor("keyMap", keyMapKeyInfo, KeyMapvalueInfo);
        this.keyMapState = getRuntimeContext().getMapState(keyMapDescriptor);
    }

    @Override
    public void processElement(Row newRow, Context ctx, Collector<Row> out) throws Exception {
        TimerService timerService = ctx.timerService();
        Long eventTime = ctx.timestamp();
        /* 没有指定eventTime或者使用ProcessTime */
        if (eventTime == null) {
            throw new RuntimeException("Please assign timestamps,watermarks and use EventTime characteristic");
        }
        Object keyFieldValue = newRow.getField(keyColumnIndex);
        if (keyMapState.contains(keyFieldValue)) {
            //merge tow rows
            Long registerTime = keyMapState.get(keyFieldValue);
            if (registerTime < eventTime) {
                log.info("registerTime < eventTime: {} < {},", registerTime, eventTime);
                processNoBufferRow(newRow, timerService, eventTime, keyFieldValue);
            } else {
                List<Row> rows = mapState.get(registerTime);
                Iterator<Row> iterator = rows.iterator();
                while (iterator.hasNext()) {
                    Row row = iterator.next();
                    Object fieldValue = row.getField(keyColumnIndex);
                    if (keyFieldValue.equals(fieldValue)) {
                        log.info("Merging row ...\n{}\n{}", newRow, row);
                        if (newRow.getArity() != row.getArity()) {
                            throw new RuntimeException("Row arity not equal");
                        } else {
                            for (int i = 0; i < newRow.getArity(); i++) {
                                Object valueFieldV = newRow.getField(i);
                                Object rowFieldV = row.getField(i);
                                row.setField(i, valueFieldV == null ? rowFieldV : valueFieldV);
                            }
                        }
                        log.info("Merge result {}", row);
                    }
                }
                mapState.put(registerTime, rows);
            }
        } else {
            processNoBufferRow(newRow, timerService, eventTime, keyFieldValue);
        }

    }

    private void processNoBufferRow(Row newRow, TimerService timerService, Long eventTime, Object keyFieldValue) throws Exception {
        List<Row> rows = mapState.get(eventTime);
        rows = rows == null ? new ArrayList<>() : rows;
        rows.add(newRow);
        long registerTime = eventTime + 1000 * 3; //3s后触发
        keyMapState.put(keyFieldValue, registerTime);
        mapState.put(registerTime, rows);
        log.info("Register timer with {}, key is {}", registerTime, keyFieldValue);
        timerService.registerEventTimeTimer(registerTime);
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<Row> out) throws Exception {
        log.info("On Timer @ {}", timestamp);
        Tuple currentKey = ctx.getCurrentKey();

        TimerService timerService = ctx.timerService();
        List<Row> rows = mapState.get(timestamp);
        Iterator<Row> iterator = rows.iterator();
        while (iterator.hasNext()) {
            Row row = iterator.next();
            out.collect(row); // collect row
            Object keyField = row.getField(keyColumnIndex);
            keyMapState.remove(keyField);
        }
        mapState.remove(timestamp);
        timerService.deleteEventTimeTimer(timestamp);
    }
}
