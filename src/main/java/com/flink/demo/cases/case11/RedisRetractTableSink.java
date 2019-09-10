package com.flink.demo.cases.case11;

import com.flink.demo.cases.case12.RedisSink;
import com.flink.demo.cases.case12.config.FlinkJedisPoolConfig;
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
public class RedisRetractTableSink implements RetractStreamTableSink<Row> {

    private RowTypeInfo rowTypeInfo;

    public RedisRetractTableSink(RowTypeInfo rowTypeInfo) {
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
        RedisRetractTableSink redisRetractTableSink = new RedisRetractTableSink(rowTypeInfo);
        return redisRetractTableSink;
    }

    @Override
    public void emitDataStream(DataStream<Tuple2<Boolean, Row>> dataStream) {
        FlinkJedisPoolConfig conf = new FlinkJedisPoolConfig.Builder().setHost("192.168.1.83").setPort(6378).build();
        dataStream.addSink(new RedisSink<Tuple2<Boolean, Row>>(
                conf,
                new RedisExampleMapper(0, "flink-redis-sink", rowTypeInfo.getFieldNames())));
    }
}
