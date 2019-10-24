package com.flink.demo.cases.case21;

import org.apache.flink.table.api.StreamQueryConfig;
import org.apache.flink.table.runtime.aggregate.ProcessFunctionWithCleanupState;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

/**
 * Created by P0007 on 2019/10/24.
 */
public class MyStatefulFiterProcessFunctoion extends ProcessFunctionWithCleanupState<Row, Row> {

    public MyStatefulFiterProcessFunctoion(StreamQueryConfig queryConfig) {
        super(queryConfig);
    }

    @Override
    public void processElement(Row value, Context ctx, Collector<Row> out) throws Exception {

    }


}
