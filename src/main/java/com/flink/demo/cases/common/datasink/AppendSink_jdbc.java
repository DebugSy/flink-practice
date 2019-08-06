package com.flink.demo.cases.common.datasink;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.table.sinks.AppendStreamTableSink;
import org.apache.flink.table.sinks.TableSink;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by DebugSy on 2019/7/29.
 */
public class AppendSink_jdbc implements AppendStreamTableSink<Row>{

    private static final Logger logger = LoggerFactory.getLogger(AppendSink_jdbc.class);

    private String[] fieldNames;

    private TypeInformation<?>[] fieldTypes;

    public AppendSink_jdbc() {
    }

    public AppendSink_jdbc(String[] fieldNames, TypeInformation<?>[] fieldTypes) {
        this.fieldNames = fieldNames;
        this.fieldTypes = fieldTypes;
    }

    @Override
    public void emitDataStream(DataStream<Row> dataStream) {
        logger.info("Print append jdbc sink.");
        dataStream.print();
    }

    @Override
    public TypeInformation<Row> getOutputType() {
        return new RowTypeInfo(fieldTypes, fieldNames);
    }

    @Override
    public String[] getFieldNames() {
        return fieldNames;
    }

    @Override
    public TypeInformation<?>[] getFieldTypes() {
        return fieldTypes;
    }

    @Override
    public TableSink<Row> configure(String[] fieldNames, TypeInformation<?>[] fieldTypes) {
        this.fieldNames = fieldNames;
        this.fieldTypes = fieldTypes;
        return new AppendSink_jdbc(fieldNames, fieldTypes);
    }
}
