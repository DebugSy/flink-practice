package com.flink.demo.cases.case15;

import com.flink.demo.cases.common.datasource.OutOfOrderDataSource;
import com.flink.demo.cases.common.datasource.UrlClickDataSource;
import com.flink.demo.cases.common.datasource.UrlClickRowDataSource;
import org.apache.calcite.runtime.SqlFunctions;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.table.api.Types;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;

/**
 * Created by P0007 on 2019/9/18.
 */
public class FlinkProcessFuncTraining {

    private static TypeInformation userClickTypeInfo = Types.ROW(
            new String[]{"username", "url", "clickTime"},
            new TypeInformation[]{
                    Types.STRING(),
                    Types.STRING(),
                    Types.SQL_TIMESTAMP()
            });

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        DataStreamSource<Row> streamSource = env.addSource(new UrlClickRowDataSource());
        SingleOutputStreamOperator filter = streamSource
                .process(new ProcessFunction() {

            final Row out =
                    new Row(3);

            @Override
            public void processElement(Object _in1, Context ctx, Collector c) throws Exception {
                Row in1 = (Row) _in1;

                boolean isNull$5 = (Timestamp) in1.getField(2) == null;
                long result$4;
                if (isNull$5) {
                    result$4 = -1L;
                } else {
                    result$4 = (long) SqlFunctions.toLong((Timestamp) in1.getField(2));
                }


                boolean isNull$1 = (String) in1.getField(0) == null;
                String result$0;
                if (isNull$1) {
                    result$0 = "";
                } else {
                    result$0 = (String) (String) in1.getField(0);
                }


                boolean isNull$3 = (String) in1.getField(1) == null;
                String result$2;
                if (isNull$3) {
                    result$2 = "";
                } else {
                    result$2 = (String) (String) in1.getField(1);
                }


                String result$7 = "user A";

                boolean isNull$9 = isNull$3 || false;
                boolean result$8;
                if (isNull$9) {
                    result$8 = false;
                } else {
                    result$8 = result$0.compareTo(result$7) == 0;
                }

                if (result$8) {


                    if (isNull$1) {
                        out.setField(0, null);
                    } else {
                        out.setField(0, result$0);
                    }


                    if (isNull$3) {
                        out.setField(1, null);
                    } else {
                        out.setField(1, result$2);
                    }


                    Timestamp result$6;
                    if (isNull$5) {
                        result$6 = null;
                    } else {
                        result$6 = SqlFunctions.internalToTimestamp(result$4);
                    }

                    if (isNull$5) {
                        out.setField(2, null);
                    } else {
                        out.setField(2, result$6);
                    }

                    c.collect(out);
                }
            }
        })
                .name("filter")
                .returns(userClickTypeInfo);

        filter.printToErr();

        env.execute("Flink ProcessFunction Training");
    }

}
