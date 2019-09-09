package com.flink.demo.cases.case11;

import com.flink.demo.cases.case12.Util;
import com.flink.demo.cases.case12.mapper.RedisCommand;
import com.flink.demo.cases.case12.mapper.RedisCommandDescription;
import com.flink.demo.cases.case12.mapper.RedisMapper;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by P0007 on 2019/9/6.
 */
public class RedisExampleMapper implements RedisMapper<Tuple2<Boolean, Row>> {

    private static final Logger logger = LoggerFactory.getLogger(RedisExampleMapper.class);

    private int keyIndex;

    private String additionaKey;

    private String[] fieldNames;

    public RedisExampleMapper(int keyIndex, String additionaKey, String[] fieldNames) {
        this.keyIndex = keyIndex;
        this.additionaKey = additionaKey;
        this.fieldNames = fieldNames;
    }

    @Override
    public RedisCommandDescription getCommandDescription() {
        return new RedisCommandDescription(RedisCommand.HMSET, additionaKey);
    }

    @Override
    public String getKeyFromData(Tuple2<Boolean, Row> row) {
        String key = row.f1.getField(keyIndex).toString();
        String rediesKey = Util.rediesKey(additionaKey, key);
        logger.info("extract key {} from {}", rediesKey, row);
        return key;
    }

    @Override
    public Map<String, String> getValueFromData(Tuple2<Boolean, Row> data) {
        Map<String, String> result = new HashMap<>();
        Row row = data.f1;
        for (int i = 0; i < fieldNames.length; i++) {
            if (i != keyIndex) {
                result.put(fieldNames[i], String.valueOf(row.getField(i)));
            }
        }
        return result;
    }


}
