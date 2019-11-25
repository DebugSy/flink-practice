package com.flink.demo.cases.case08;

import com.flink.demo.cases.common.datasource.UrlClickRowDataSource;
import com.flink.demo.cases.common.functions.udf.Timestamp2Timezone;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by DebugSy on 2019/7/10.
 * <p>
 * java case 08
 * Flink SQL 训练 - 自定义函数
 * TUMBLE 滑动窗口
 * HOP 滚动窗口
 */
public class FlinkSqlTraining_custom_function {

    private static final Logger logger = LoggerFactory.getLogger(FlinkSqlTraining_custom_function.class);

    //CONCAT('yyyy-MM-dd', U&'\0054', 'HH:mm:ss.SSSZ') 使用unicode

    private static String tumbleWindowSql = "select U&'\\0026',username,udf_test(username) as subname " +
            "from clicks";

    private static String hopWindowSql = "select username, count(*) as cnt, " +
            "HOP_ROWTIME(rowtime, INTERVAL '5' SECOND, INTERVAL '10' SECOND) as window_rowtime, " +
            "HOP_START(rowtime, INTERVAL '5' SECOND, INTERVAL '10' SECOND) as window_start, " +
            "HOP_END(rowtime, INTERVAL '5' SECOND, INTERVAL '10' SECOND) as window_end " +
            "from clicks " +
            "where url like '%/api/H%'" +
            "group by username, " +
            "HOP(rowtime, INTERVAL '5' SECOND, INTERVAL '10' SECOND)";

    private static String sessionWindowSql = "";

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        tableEnv.registerFunction("udf_test", new Timestamp2Timezone());

        DataStream<Row> sourceStream = env
                .addSource(new UrlClickRowDataSource())
                .returns(UrlClickRowDataSource.USER_CLICK_TYPEINFO);
        TypeInformation<Row> type = sourceStream.getType();

        tableEnv.registerDataStream("clicks", sourceStream, UrlClickRowDataSource.CLICK_FIELDS_WITH_ROWTIME);

        Table sqlQuery = tableEnv.sqlQuery(hopWindowSql);

        DataStream<Tuple2<Boolean, Row>> sinkStream = tableEnv.toRetractStream(sqlQuery, Row.class);
        sinkStream.printToErr();


        env.execute("Flink SQL Training");
    }

}
