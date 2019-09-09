package com.flink.demo.cases.case12;

import com.flink.demo.cases.case12.config.FlinkJedisConfigBase;
import com.flink.demo.cases.case12.container.RedisCommandsContainer;
import com.flink.demo.cases.case12.container.RedisCommandsContainerBuilder;
import com.flink.demo.cases.case12.mapper.RedisCommand;
import com.flink.demo.cases.case12.mapper.RedisCommandDescription;
import com.flink.demo.cases.case12.mapper.RedisMapper;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * 自定义flink-redis-connector
 * 主要是为了兼容spark-redis和flink-redis
 *
 * 参照：bahir-flink  https://github.com/apache/bahir-flink
 *       spark-redis  http://github.com/RedisLabs/spark-redis
 * @param <IN>
 */

public class RedisSink<IN> extends RichSinkFunction<IN> {

    private static final Logger logger = LoggerFactory.getLogger(RedisSink.class);

    private String additionalKey;
    private RedisMapper<IN> redisSinkMapper;
    private RedisCommand redisCommand;

    private FlinkJedisConfigBase flinkJedisConfigBase;
    private RedisCommandsContainer redisCommandsContainer;

    public RedisSink(FlinkJedisConfigBase flinkJedisConfigBase, RedisMapper<IN> redisSinkMapper) {
        Objects.requireNonNull(flinkJedisConfigBase, "Redis connection pool config should not be null");
        Objects.requireNonNull(redisSinkMapper, "Redis Mapper can not be null");
        Objects.requireNonNull(redisSinkMapper.getCommandDescription(), "Redis Mapper data type description can not be null");

        this.flinkJedisConfigBase = flinkJedisConfigBase;

        this.redisSinkMapper = redisSinkMapper;
        RedisCommandDescription redisCommandDescription = redisSinkMapper.getCommandDescription();
        this.redisCommand = redisCommandDescription.getCommand();
        this.additionalKey = redisCommandDescription.getAdditionalKey();
    }

    @Override
    public void invoke(IN input, Context context) throws Exception {
        String key = redisSinkMapper.getKeyFromData(input);
        Map<String, String> valueMap = redisSinkMapper.getValueFromData(input);
        Optional<String> optAdditionalKey = redisSinkMapper.getAdditionalKey(input);
        switch (redisCommand) {
            case HMSET:
                this.redisCommandsContainer.hset(key, valueMap);
                break;
            default:
                throw new IllegalArgumentException("Cannot process such data type: " + redisCommand);
        }
    }

    /**
     * Initializes the connection to Redis by either cluster or sentinels or single server.
     *
     * @throws IllegalArgumentException if jedisPoolConfig, jedisClusterConfig and jedisSentinelConfig are all null
     */
    @Override
    public void open(Configuration parameters) throws Exception {
        try {
            this.redisCommandsContainer = RedisCommandsContainerBuilder.build(this.flinkJedisConfigBase);
            this.redisCommandsContainer.open();
        } catch (Exception e) {
            logger.error("Redis has not been properly initialized: ", e);
            throw e;
        }
    }

    /**
     * Closes commands container.
     * @throws IOException if command container is unable to close.
     */
    @Override
    public void close() throws IOException {
        if (redisCommandsContainer != null) {
            redisCommandsContainer.close();
        }
    }
}
