package com.flink.demo.cases.case02;

import com.flink.demo.cases.common.datasource.UrlClickRowDataSource;
import com.flink.demo.cases.common.datasource.UserRowDataSource;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.Types;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import java.sql.Timestamp;

/**
 * Created by P0007 on 2019/10/12.
 *
 * join的实现是基于coGroup实现的
 * org.apache.flink.streaming.api.datastream.JoinedStreams:457
 *
 * public void coGroup(Iterable<T1> first, Iterable<T2> second, Collector<T> out) throws Exception {
 * 			for (T1 val1: first) {
 * 				for (T2 val2: second) {
 * 					out.collect(wrappedFunction.join(val1, val2));
 *              }
 *          }
 *  }
 */
public class FlinkSqlTraining_join_row_copy {

    /**
     * 两个流基于时间窗口的join
     */
    private static String innerJoinWithTimeWindowSql = "insert into sink_console " +
            "SELECT c.userId,c.username as username_c,u.username as username_u,url,clickTime,data_col,time_col,address,activityTime " +
            "FROM clicks c left join users u " +
            "on c.userId = u.userId " +
            "AND c.clickTime BETWEEN u.activityTime - INTERVAL '60' SECOND AND u.activityTime";

    public static TypeInformation JOIN_RESULT = Types.ROW(
            new String[]{"userId", "username_c", "username_u", "url", "clickTime", "data_col", "time_col", "address", "activityTime"},
            new TypeInformation[]{
                    Types.INT(),
                    Types.STRING(),
                    Types.STRING(),
                    Types.STRING(),
                    Types.SQL_TIMESTAMP(),
                    Types.STRING(),
                    Types.STRING(),
                    Types.STRING(),
                    Types.SQL_TIMESTAMP()
            });

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        SingleOutputStreamOperator<Row> clickStream = env.addSource(new UrlClickRowDataSource())
                .returns(UrlClickRowDataSource.USER_CLICK_TYPEINFO);
        SingleOutputStreamOperator<Row> clickStreamAndWatermarks = clickStream.assignTimestampsAndWatermarks(new AscendingTimestampExtractor<Row>() {
            @Override
            public long extractAscendingTimestamp(Row element) {
                return Timestamp.valueOf(element.getField(3).toString()).getTime();
            }
        });

        SingleOutputStreamOperator<Row> userStream = env.addSource(new UserRowDataSource())
                .returns(UserRowDataSource.USER_TYPEINFO);
        SingleOutputStreamOperator<Row> userStreamAndWatermarks = userStream.assignTimestampsAndWatermarks(new AscendingTimestampExtractor<Row>() {
            @Override
            public long extractAscendingTimestamp(Row element) {
                return Timestamp.valueOf(element.getField(3).toString()).getTime();
            }
        });

        StreamTableEnvironment tableEnv = TableEnvironment.getTableEnvironment(env);
        tableEnv.registerDataStream("clicks", clickStreamAndWatermarks, UrlClickRowDataSource.CLICK_FIELDS);
        tableEnv.registerDataStream("users", userStreamAndWatermarks, UserRowDataSource.USER_FIELDS);

        tableEnv.registerTableSink("sink_console", new ConsoleTableSink((RowTypeInfo) JOIN_RESULT));

        tableEnv.sqlUpdate(innerJoinWithTimeWindowSql);

        env.execute("Flink Stream Join Training");
    }

}
