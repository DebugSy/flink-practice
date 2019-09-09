package com.flink.demo.cases.case12.mapper;

import org.apache.flink.api.common.functions.Function;

import java.io.Serializable;
import java.util.Map;
import java.util.Optional;

public interface RedisMapper<T> extends Function, Serializable {

    /**
     * Returns descriptor which defines data type.
     *
     * @return data type descriptor
     */
    RedisCommandDescription getCommandDescription();

    /**
     * Extracts key from data.
     *
     * @param data source data
     * @return key
     */
    String getKeyFromData(T data);

    /**
     * Extracts value from data.
     *
     * @param data source data
     * @return value
     */
    Map<String, String> getValueFromData(T data);

    /**
     * Extracts the additional key from data as an {@link Optional < String >}.
     * The default implementation returns an empty Optional.
     *
     * @param data
     * @return Optional
     */
    default Optional<String> getAdditionalKey(T data) {
        return Optional.empty();
    }
}
