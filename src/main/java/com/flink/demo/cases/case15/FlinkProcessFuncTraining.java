//package com.flink.demo.cases.case15;
//
//import com.flink.demo.cases.common.datasource.UrlClickRowDataSource;
//import org.apache.flink.api.common.typeinfo.TypeInformation;
//import org.apache.flink.streaming.api.TimeCharacteristic;
//import org.apache.flink.streaming.api.datastream.DataStreamSource;
//import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
//import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
//import org.apache.flink.table.api.Types;
//import org.apache.flink.types.Row;
//
///**
// * Created by P0007 on 2019/9/18.
// */
//public class FlinkProcessFuncTraining {
//
//    private static TypeInformation userClickTypeInfo = Types.ROW(
//            new String[]{"userId", "username", "url", "clickTime"},
//            new TypeInformation[]{
//                    Types.INT(),
//                    Types.STRING(),
//                    Types.STRING(),
//                    Types.SQL_TIMESTAMP()
//            });
//
//    public static void main(String[] args) throws Exception {
//        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        env.setParallelism(1);
//        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
//
//        DataStreamSource<Row> streamSource = env.addSource(new UrlClickRowDataSource());
//        SingleOutputStreamOperator filter = streamSource
//                .process(new MyProcessFunction$10())
//                .name("filter")
//                .returns(userClickTypeInfo);
//
//        filter.printToErr();
//
//        env.execute("Flink ProcessFunction Training");
//    }
//
//}
