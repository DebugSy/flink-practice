package com.flink.demo.cases.case13;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.table.sinks.RetractStreamTableSink;
import org.apache.flink.table.sinks.TableSink;
import org.apache.flink.types.Row;

/**
 * Created by P0007 on 2019/9/9.
 */
public class RetractTableSink implements RetractStreamTableSink<Row> {

    private RowTypeInfo rowTypeInfo;

    public RetractTableSink(RowTypeInfo rowTypeInfo) {
        this.rowTypeInfo = rowTypeInfo;
    }

    @Override
    public TypeInformation<Row> getRecordType() {
        return rowTypeInfo;
    }

    @Override
    public TupleTypeInfo<Tuple2<Boolean, Row>> getOutputType() {
        return new TupleTypeInfo<>(Types.BOOLEAN, rowTypeInfo);
    }

    @Override
    public String[] getFieldNames() {
        return rowTypeInfo.getFieldNames();
    }

    @Override
    public TypeInformation<?>[] getFieldTypes() {
        return rowTypeInfo.getFieldTypes();
    }

    @Override
    public TableSink configure(String[] fieldNames, TypeInformation[] fieldTypes) {
        RetractTableSink redisRetractTableSink = new RetractTableSink(rowTypeInfo);
        return redisRetractTableSink;
    }

    @Override
    public void emitDataStream(DataStream<Tuple2<Boolean, Row>> dataStream) {
        dataStream.printToErr();
    }
}
