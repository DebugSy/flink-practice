package com.flink.demo.cases.case12.container;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisSentinelPool;

import java.io.Closeable;
import java.io.IOException;
import java.util.Map;
import java.util.Objects;

/**
 * Redis command container if we want to connect to a single Redis server or to Redis sentinels
 * If want to connect to a single Redis server, please use the first constructor {@link #RedisContainer(JedisPool)}.
 * If want to connect to a Redis sentinels, please use the second constructor {@link #RedisContainer(JedisSentinelPool)}
 */
public class RedisContainer implements RedisCommandsContainer, Closeable {

    private static final Logger logger = LoggerFactory.getLogger(RedisContainer.class);

    private transient JedisPool jedisPool;
    private transient JedisSentinelPool jedisSentinelPool;

    /**
     * Use this constructor if to connect with single Redis server.
     *
     * @param jedisPool JedisPool which actually manages Jedis instances
     */
    public RedisContainer(JedisPool jedisPool) {
        Objects.requireNonNull(jedisPool, "Jedis Pool can not be null");
        this.jedisPool = jedisPool;
        this.jedisSentinelPool = null;
    }

    /**
     * Use this constructor if Redis environment is clustered with sentinels.
     *
     * @param sentinelPool SentinelPool which actually manages Jedis instances
     */
    public RedisContainer(final JedisSentinelPool sentinelPool) {
        Objects.requireNonNull(sentinelPool, "Jedis Sentinel Pool can not be null");
        this.jedisPool = null;
        this.jedisSentinelPool = sentinelPool;
    }

    /**
     * Closes the Jedis instances.
     */
    @Override
    public void close() throws IOException {
        if (this.jedisPool != null) {
            this.jedisPool.close();
        }
        if (this.jedisSentinelPool != null) {
            this.jedisSentinelPool.close();
        }
    }

    @Override
    public void open() throws Exception {

        // echo() tries to open a connection and echos back the
        // message passed as argument. Here we use it to monitor
        // if we can communicate with the cluster.

        getInstance().echo("Test");
    }

    @Override
    public Map<String, String> hgetAll(String key) {
        Jedis jedis = null;
        try {
            jedis = getInstance();
            Map<String, String> value = jedis.hgetAll(key);
            return value;
        } catch (Exception e) {
            if (logger.isErrorEnabled()) {
                logger.error("Cannot send Redis message with command HGET to key {} error message {}",
                        key, e.getMessage());
            }
            throw e;
        } finally {
            releaseInstance(jedis);
        }
    }

    @Override
    public void hset(final String key, final Map<String, String> value) {
        Jedis jedis = null;
        try {
            jedis = getInstance();
            jedis.hmset(key, value);
        } catch (Exception e) {
            if (logger.isErrorEnabled()) {
                logger.error("Cannot send Redis message with command HMSET to key {} error message {}",
                        key, e.getMessage());
            }
            throw e;
        } finally {
            releaseInstance(jedis);
        }
    }

    @Override
    public void set(final String key, final String value) {
        Jedis jedis = null;
        try {
            jedis = getInstance();
            jedis.set(key, value);
        } catch (Exception e) {
            if (logger.isErrorEnabled()) {
                logger.error("Cannot send Redis message with command SET to key {} error message {}",
                    key, e.getMessage());
            }
            throw e;
        } finally {
            releaseInstance(jedis);
        }
    }

    /**
     * Returns Jedis instance from the pool.
     *
     * @return the Jedis instance
     */
    private Jedis getInstance() {
        if (jedisSentinelPool != null) {
            return jedisSentinelPool.getResource();
        } else {
            return jedisPool.getResource();
        }
    }

    /**
     * Closes the jedis instance after finishing the command.
     *
     * @param jedis The jedis instance
     */
    private void releaseInstance(final Jedis jedis) {
        if (jedis == null) {
            return;
        }
        try {
            jedis.close();
        } catch (Exception e) {
            logger.error("Failed to close (return) instance to pool", e);
        }
    }
}
