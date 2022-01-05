package com.flink.demo.cases.case00;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.api.Types;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;

public class SplitUDTF extends TableFunction<Row> {

    private final Row result = new Row(1);

    public void eval(String column) {
        String[] strings = column.split("");
        for (String str : strings) {
            result.setField(0, str);
            collect(result);
        }

        System.err.println("---------------");
    }

    @Override
    public TypeInformation<Row> getResultType() {
        return Types.ROW(Types.STRING());
    }
}
