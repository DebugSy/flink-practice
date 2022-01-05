package com.flink.demo.cases.case02;

import org.apache.flink.table.functions.TableFunction;

public class SplitUDTF extends TableFunction<String> {

    public void eval(String column, String separator, String... name) {
        String[] strings = column.split(separator);
        for (String str : strings) {
            collect(str);
        }
    }

}
