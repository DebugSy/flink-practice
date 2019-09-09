package com.flink.demo.cases.case12;

public class Util {
    public static void checkArgument(boolean condition, String message) {
        if(!condition) {
            throw new IllegalArgumentException(message);
        }
    }

    public static String rediesKey(String additionalKey, String key) {
        return additionalKey + ":" + key;
    }
}
