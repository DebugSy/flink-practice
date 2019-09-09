package com.flink.demo.cases.case11;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.table.sinks.AppendStreamTableSink;
import org.apache.flink.table.sinks.TableSink;
import org.apache.flink.types.Row;

/**
 * Created by P0007 on 2019/9/9.
 */
public class RedisAppendTableSink implements AppendStreamTableSink<Row> {

    private RowTypeInfo rowTypeInfo;

    public RedisAppendTableSink(RowTypeInfo rowTypeInfo) {
        this.rowTypeInfo = rowTypeInfo;
    }


    @Override
    public TypeInformation<Row> getOutputType() {
        return Types.ROW_NAMED(rowTypeInfo.getFieldNames(), rowTypeInfo.getFieldTypes());
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
        RedisAppendTableSink redisTableSink = new RedisAppendTableSink(rowTypeInfo);
        return redisTableSink;
    }

    @Override
    public void emitDataStream(DataStream dataStream) {
        dataStream.printToErr();
    }
}
