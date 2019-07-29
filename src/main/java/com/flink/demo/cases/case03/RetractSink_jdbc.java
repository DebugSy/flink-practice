package com.flink.demo.cases.case03;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.table.sinks.RetractStreamTableSink;
import org.apache.flink.table.sinks.TableSink;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RetractSink_jdbc implements RetractStreamTableSink<Row> {

    private static final Logger logger = LoggerFactory.getLogger("");

    private String[] fieldNames;

    private TypeInformation<?>[] typeInformations;

    public RetractSink_jdbc() {
    }

    public RetractSink_jdbc(String[] fieldNames, TypeInformation<?>[] typeInformations) {
        this.fieldNames = fieldNames;
        this.typeInformations = typeInformations;
    }

    @Override
    public TypeInformation getRecordType() {
        return Types.ROW_NAMED(fieldNames, typeInformations);
    }

    @Override
    public TupleTypeInfo<Tuple2<Boolean, Row>> getOutputType() {
        return new TupleTypeInfo<>(Types.BOOLEAN, getRecordType());
    }

    @Override
    public String[] getFieldNames() {
        return fieldNames;
    }

    @Override
    public TypeInformation<?>[] getFieldTypes() {
        return typeInformations;
    }

    @Override
    public TableSink configure(String[] fieldNames, TypeInformation[] fieldTypes) {
        RetractSink_jdbc retractSink_jdbc = new RetractSink_jdbc(fieldNames, fieldTypes);
        return retractSink_jdbc;
    }

    @Override
    public void emitDataStream(DataStream dataStream) {

        dataStream.printToErr();
    }
}
