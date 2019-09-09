package com.flink.demo.cases.case11;

import com.flink.demo.cases.case12.RedisLookup;
import com.flink.demo.cases.case12.config.FlinkJedisPoolConfig;
import com.flink.demo.cases.common.datasource.UrlClickDataSource;
import com.flink.demo.cases.common.datasource.UrlClickRowDataSource;
import com.flink.demo.cases.common.functions.udtf.UserTableFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.Types;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Timestamp;

import static com.flink.demo.cases.common.datasource.UrlClickRowDataSource.CLICK_FIELDS;

/**
 * Created by DebugSy on 2019/7/10.
 *
 * java case 11
 * Flink Redis Lookup 训练
 * 与静态表join
 */
public class FlinkRedisConnectorTraining_lookup {

    private static final Logger logger = LoggerFactory.getLogger(FlinkRedisConnectorTraining_lookup.class);

    private static TypeInformation userClickTypeInfo = Types.ROW(
            new String[]{"userId", "username", "url", "clickTime"},
            new TypeInformation[]{
                    Types.INT(),
                    Types.STRING(),
                    Types.STRING(),
                    Types.SQL_TIMESTAMP()
            });

    private static TypeInformation lookupTypeInfo = Types.ROW(
            new String[]{"username", "cnt"},
            new TypeInformation[]{
                    Types.STRING(),
                    Types.STRING()
            });

    /**
     * 流连接侧表，类似lookup
     */
    private static String innerJoinSql = "select userId,username,url,clickTime,cnt from clicks, LATERAL TABLE(users(username)) as T(name,cnt)";

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        StreamTableEnvironment tableEnv = TableEnvironment.getTableEnvironment(env);

        DataStream<Row> streamSource = env.addSource(new UrlClickRowDataSource()).returns(userClickTypeInfo);

        tableEnv.registerDataStream("clicks", streamSource, CLICK_FIELDS);

        FlinkJedisPoolConfig conf = new FlinkJedisPoolConfig.Builder().setHost("192.168.1.83").setPort(6378).build();
        RedisLookup redisLookup = new RedisLookup(conf, (RowTypeInfo) lookupTypeInfo, "flink-sink-3");
        tableEnv.registerFunction("users", redisLookup);

        Table sqlQuery = tableEnv.sqlQuery(innerJoinSql);

        DataStream<Row> sinkStream = tableEnv.toAppendStream(sqlQuery, Row.class);
        sinkStream.printToErr();


        env.execute("Flink SQL Training");
    }

}
