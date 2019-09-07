package com.flink.demo.cases.case11;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by P0007 on 2019/9/6.
 */
public class RedisExampleMapper implements RedisMapper<Tuple2<Boolean, Row>> {

    private static final Logger logger = LoggerFactory.getLogger(RedisExampleMapper.class);

    private int keyIndex;

    private String additionaKey;

    public RedisExampleMapper(int keyIndex, String additionaKey) {
        this.keyIndex = keyIndex;
        this.additionaKey = additionaKey;
    }

    @Override
    public RedisCommandDescription getCommandDescription() {
        return new RedisCommandDescription(RedisCommand.HSET, additionaKey);
    }

    @Override
    public String getKeyFromData(Tuple2<Boolean, Row> row) {
        String key = row.f1.getField(keyIndex).toString();
        logger.info("extract key {} from {}", key, row);
        return key;
    }

    @Override
    public String getValueFromData(Tuple2<Boolean, Row> row) {
        StringBuilder stringBuilder = new StringBuilder();
        int arity = row.getArity();
        for (int i = 0; i < arity; i++) {
            if (i != keyIndex) {
                stringBuilder.append(row.f1.getField(i));
                stringBuilder.append(",");
            }
        }
        logger.info("extract value {} from {}", stringBuilder.toString(), row);
        return stringBuilder.toString();
    }


}
