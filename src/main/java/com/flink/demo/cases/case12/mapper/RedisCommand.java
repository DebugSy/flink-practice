package com.flink.demo.cases.case12.mapper;

public enum RedisCommand {

    /**
     * Sets field in the hash stored at key to value. If key does not exist,
     * a new key holding a hash is created. If field already exists in the hash, it is overwritten.
     */
    HMSET(RedisDataType.HASH);

    /**
     * The {@link RedisDataType} this command belongs to.
     */
    private RedisDataType redisDataType;

    RedisCommand(RedisDataType redisDataType) {
        this.redisDataType = redisDataType;
    }


    /**
     * The {@link RedisDataType} this command belongs to.
     * @return the {@link RedisDataType}
     */
    public RedisDataType getRedisDataType(){
        return redisDataType;
    }
}
