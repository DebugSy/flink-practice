package com.flink.demo.cases.case12;

import com.flink.demo.cases.case12.config.FlinkJedisConfigBase;
import com.flink.demo.cases.case12.container.RedisCommandsContainer;
import com.flink.demo.cases.case12.container.RedisCommandsContainerBuilder;
import com.flink.demo.cases.case12.mapper.RedisCommand;
import com.flink.demo.cases.common.utils.TypeInfoUtil;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.table.api.Types;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * Created by P0007 on 2019/9/9.
 */
public class RedisLookup extends TableFunction<Row> {

    private static final Logger logger = LoggerFactory.getLogger(RedisLookup.class);

    private String additionalKey;

    private FlinkJedisConfigBase flinkJedisConfigBase;
    private RedisCommandsContainer redisCommandsContainer;

    private RowTypeInfo rowTypeInfo;

    public RedisLookup(FlinkJedisConfigBase flinkJedisConfigBase, RowTypeInfo rowTypeInfo, String additionalKey) {
        this.flinkJedisConfigBase = flinkJedisConfigBase;
        this.rowTypeInfo = rowTypeInfo;
        this.additionalKey = additionalKey;
    }

    @Override
    public void open(FunctionContext context) throws Exception {
        try {
            this.redisCommandsContainer = RedisCommandsContainerBuilder.build(this.flinkJedisConfigBase);
            this.redisCommandsContainer.open();
        } catch (Exception e) {
            logger.error("Redis has not been properly initialized: ", e);
            throw e;
        }
    }

    public void eval(String key) {
        String rediesKey = Util.rediesKey(additionalKey, key);
        Map<String, String> value = redisCommandsContainer.hgetAll(rediesKey);
        String[] fieldNames = rowTypeInfo.getFieldNames();
        Row row = new Row(fieldNames.length);
        row.setField(0, TypeInfoUtil.cast(key, rowTypeInfo.getTypeAt(0)));//这里还需要将类型转换
        for (int i = 1; i < fieldNames.length; i++) {
            String valueStr = value.get(fieldNames[i]);
            TypeInformation<Object> fieldType = rowTypeInfo.getTypeAt(fieldNames[i]);
            Object castedValue = TypeInfoUtil.cast(valueStr, fieldType);
            row.setField(i, castedValue);//这里还需要将类型转换
        }
        collect(row);
    }

    @Override
    public TypeInformation<Row> getResultType() {
        return rowTypeInfo;
    }
}