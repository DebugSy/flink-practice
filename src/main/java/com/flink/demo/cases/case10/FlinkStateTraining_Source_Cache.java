package com.flink.demo.cases.case10;

import com.flink.demo.cases.common.datasource.UrlClickRowDataSource;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Types;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.SimpleDateFormat;

import static com.flink.demo.cases.common.datasource.UrlClickRowDataSource.USER_CLICK_TYPEINFO;

/**
 * Created by DebugSy on 2020/03/16.
 * <p>
 * java case 26
 * Flink SQL 训练 - 在source 处缓存一段时间的数据
 * 实际场景：中移金科那边kafka中的数据一个流水号会有两条数据过来，两条数据是有间隔的，需要将两条数据合并
 */
public class FlinkStateTraining_Source_Cache {

    private static final Logger logger = LoggerFactory.getLogger(FlinkStateTraining_Source_Cache.class);

    private static String tumbleWindowSql = "select username, count(*) as cnt, " +
            "TUMBLE_START(watermark_col, INTERVAL '10' SECOND) as window_start, " +
            "TUMBLE_END(watermark_col, INTERVAL '10' SECOND) as window_end, " +
            "SUBSTRING(TIMESTAMP_TO_STRING(TUMBLE_START(watermark_col, INTERVAL '10' SECOND)), 1, 14) AS PATH_DIR " +
            "from new_clicks " +
            "group by username, " +
            "TUMBLE(watermark_col, INTERVAL '10' SECOND)";

    private static SimpleDateFormat dateFormat = new SimpleDateFormat("yyyyMMddHHmmss");

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        SingleOutputStreamOperator<Row> sourceStream = env.addSource(new UrlClickRowDataSource()).returns(USER_CLICK_TYPEINFO);
        CacheStreamFunction cacheStreamFunction = new CacheStreamFunction("userId", (RowTypeInfo) USER_CLICK_TYPEINFO);
        SingleOutputStreamOperator<Row> cacheStream = sourceStream.keyBy(0).process(cacheStreamFunction);

        cacheStream.printToErr();

        env.execute("Flink SQL Training");
    }


    public static TypeInformation USER_CLICK_TYPEINFO_WATERMARK = Types.ROW(
            new String[]{"userId", "username", "url", "clickTime", "data_col", "time_col", "watermark_col"},
            new TypeInformation[]{
                    Types.INT(),
                    Types.STRING(),
                    Types.STRING(),
                    Types.SQL_TIMESTAMP(),
                    Types.STRING(),
                    Types.STRING(),
                    Types.LONG()
            });

}
