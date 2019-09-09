package com.flink.demo.cases.case12.container;

import java.io.IOException;
import java.io.Serializable;
import java.util.Map;

/**
 * The container for all available Redis commands.
 */
public interface RedisCommandsContainer extends Serializable {

    /**
     * Open the Jedis container.
     *
     * @throws Exception if the instance can not be opened properly
     */
    void open() throws Exception;


    /**
     * Get map values
     * @param key
     * @return
     */
    Map<String, String> hgetAll(String key);


    /**
     * Sets field in the hash stored at key to value.
     * If key does not exist, a new key holding a hash is created.
     * If field already exists in the hash, it is overwritten.
     *
     * @param key Hash name
     * @param value Hash value map
     */
    void hset(String key, Map<String, String> value);

    /**
     * Set key to hold the string value. If key already holds a value, it is overwritten,
     * regardless of its type. Any previous time to live associated with the key is
     * discarded on successful SET operation.
     *
     * @param key the key name in which value to be set
     * @param value the value
     */
    void set(String key, String value);

    /**
     * Close the Jedis container.
     *
     * @throws IOException if the instance can not be closed properly
     */
    void close() throws IOException;
}
