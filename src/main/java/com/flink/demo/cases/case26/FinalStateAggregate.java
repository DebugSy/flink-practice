package com.flink.demo.cases.case26;

import com.flink.demo.cases.common.utils.ClassUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import java.util.Iterator;

/**
 * @author P0007
 * @version 1.0
 * @date 2020/4/21 18:57
 */
@Slf4j
public class FinalStateAggregate extends ProcessWindowFunction<Row, Row, Tuple, TimeWindow> {

    private RowTypeInfo rowTypeInfo;

    public FinalStateAggregate(RowTypeInfo rowTypeInfo) {
        this.rowTypeInfo = rowTypeInfo;
    }

    @Override
    public void process(Tuple tuple, Context context, Iterable<Row> elements, Collector<Row> out) throws Exception {
        //TODO 窗口数据是否按watermark排序？
        Iterator<Row> iterator = elements.iterator();
        if (iterator.hasNext()) {
            Row mergedRow = iterator.next();
            while (iterator.hasNext()) {
                Row row = iterator.next();
                log.debug("Merging row :\n{}\n{}", row, mergedRow);
                if (row.getArity() != mergedRow.getArity()) {
                    throw new RuntimeException("Row arity not equal");
                } else {
                    for (int i = 0; i < row.getArity(); i++) {
                        Object mergedRowFieldV = mergedRow.getField(i);
                        Object rowFieldV = row.getField(i);
                        String type = rowTypeInfo.getTypeAt(i).toString();
                        mergedRow.setField(i, ClassUtil.isEmpty(mergedRowFieldV, type) ? rowFieldV : mergedRowFieldV);
                    }
                }
            }
            log.debug("Merged result {}", mergedRow);
            out.collect(mergedRow);
        }
    }
}