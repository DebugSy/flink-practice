package com.flink.demo.cases.case12.mapper;

import java.io.Serializable;
import java.util.Objects;

public class RedisCommandDescription implements Serializable {

    private RedisCommand redisCommand;

    private String additionalKey;

    public RedisCommandDescription(RedisCommand redisCommand, String additionalKey) {
        Objects.requireNonNull(redisCommand, "Redis command type can not be null");
        this.redisCommand = redisCommand;
        this.additionalKey = additionalKey;

        if (redisCommand.getRedisDataType() == RedisDataType.HASH ||
            redisCommand.getRedisDataType() == RedisDataType.SORTED_SET) {
            if (additionalKey == null) {
                throw new IllegalArgumentException("Hash and Sorted Set should have additional key");
            }
        }
    }

    public RedisCommandDescription(RedisCommand redisCommand) {
        this(redisCommand, null);
    }


    public RedisCommand getCommand() {
        return redisCommand;
    }


    public String getAdditionalKey() {
        return additionalKey;
    }
}
