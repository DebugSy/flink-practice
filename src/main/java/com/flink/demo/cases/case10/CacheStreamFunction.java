package com.flink.demo.cases.case10;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.functions.IterationRuntimeContext;
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
        TypeInformation<Object> keyInfo = TypeInformation.of(Object.class);
        TypeInformation<List<Row>> valueInfo = TypeInformation.of(new TypeHint<List<Row>>() {
        });
        MapStateDescriptor stateDescriptor = new MapStateDescriptor("cache", keyInfo, valueInfo);
        this.mapState = getRuntimeContext().getMapState(stateDescriptor);

        TypeInformation<Object> keyMapKeyInfo = TypeInformation.of(Object.class);
        TypeInformation<Long> KeyMapvalueInfo = TypeInformation.of(Long.class);
        MapStateDescriptor keyMapDescriptor = new MapStateDescriptor("cache", keyMapKeyInfo, KeyMapvalueInfo);
        this.keyMapState = getRuntimeContext().getMapState(keyMapDescriptor);
    }

    @Override
    public void processElement(Row value, Context ctx, Collector<Row> out) throws Exception {
        TimerService timerService = ctx.timerService();
        Long eventTime = ctx.timestamp();
        Object keyField = value.getField(keyColumnIndex);
        if (keyMapState.contains(keyField)) {
            //TODO merge value
            Long registerTime = keyMapState.get(keyField);
            List<Row> rows = mapState.get(registerTime);
            Iterator<Row> iterator = rows.iterator();
            while (iterator.hasNext()) {
                Row row = iterator.next();
                Object field = row.getField(keyColumnIndex);
                if (keyField.equals(field)) {
                    log.info("Merge row ...");
                }
            }
        } else {
            List<Row> rows = mapState.get(eventTime);
            if (rows == null) {
                rows = new ArrayList<>();
            }
            rows.add(value);
            long registerTime = eventTime + 1000 * 10; //10s后触发
            keyMapState.put(keyField, registerTime);
            mapState.put(registerTime, rows);
            log.info("Register timer with {}", registerTime);
            timerService.registerEventTimeTimer(registerTime);
        }

    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<Row> out) throws Exception {
        log.info("On Timer @ {}", timestamp);
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
        log.info("MapState is {}", mapState);

    }
}
