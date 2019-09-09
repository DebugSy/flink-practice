/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.flink.demo.cases.case12.container;

import com.flink.demo.cases.case12.config.FlinkJedisConfigBase;
import com.flink.demo.cases.case12.config.FlinkJedisPoolConfig;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import redis.clients.jedis.JedisPool;

import java.util.Objects;

/**
 * The builder for {@link RedisCommandsContainer}.
 */
public class RedisCommandsContainerBuilder {

    public static RedisCommandsContainer build(FlinkJedisConfigBase flinkJedisConfigBase){
        if(flinkJedisConfigBase instanceof FlinkJedisPoolConfig){
            FlinkJedisPoolConfig flinkJedisPoolConfig = (FlinkJedisPoolConfig) flinkJedisConfigBase;
            return RedisCommandsContainerBuilder.build(flinkJedisPoolConfig);
        } else {
            throw new IllegalArgumentException("Jedis configuration not found");
        }
    }

    /**
     * Builds container for single Redis environment.
     *
     * @param jedisPoolConfig configuration for JedisPool
     * @return container for single Redis environment
     * @throws NullPointerException if jedisPoolConfig is null
     */
    public static RedisCommandsContainer build(FlinkJedisPoolConfig jedisPoolConfig) {
        Objects.requireNonNull(jedisPoolConfig, "Redis pool config should not be Null");

        GenericObjectPoolConfig genericObjectPoolConfig = new GenericObjectPoolConfig();
        genericObjectPoolConfig.setMaxIdle(jedisPoolConfig.getMaxIdle());
        genericObjectPoolConfig.setMaxTotal(jedisPoolConfig.getMaxTotal());
        genericObjectPoolConfig.setMinIdle(jedisPoolConfig.getMinIdle());

        JedisPool jedisPool = new JedisPool(genericObjectPoolConfig, jedisPoolConfig.getHost(),
            jedisPoolConfig.getPort(), jedisPoolConfig.getConnectionTimeout(), jedisPoolConfig.getPassword(),
            jedisPoolConfig.getDatabase());
        return new RedisContainer(jedisPool);
    }
}
