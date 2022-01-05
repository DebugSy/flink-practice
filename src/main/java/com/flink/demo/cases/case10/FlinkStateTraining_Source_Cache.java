package com.flink.demo.cases.case10;

import com.flink.demo.cases.common.datasource.UrlClickRowDataSource;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.table.api.Types;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Timestamp;
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
            "TUMBLE_START(watermark_col, INTERVAL '60' SECOND) as window_start, " +
            "TUMBLE_END(watermark_col, INTERVAL '60' SECOND) as window_end, " +
            "SUBSTRING(TIMESTAMP_TO_STRING(TUMBLE_START(watermark_col, INTERVAL '60' SECOND)), 1, 14) AS PATH_DIR " +
            "from new_clicks " +
            "group by username, " +
            "TUMBLE(watermark_col, INTERVAL '60' SECOND)";

    private static SimpleDateFormat dateFormat = new SimpleDateFormat("yyyyMMddHHmmss");

    public static void main(String[] args) throws Exception {
        Configuration configuration = new Configuration();
        configuration.setString("state.checkpoints.num-retained", "10");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(configuration);
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        //checkpoint config
        env.enableCheckpointing(1000 * 5);
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(500);
        env.getCheckpointConfig().setCheckpointTimeout(1000 * 10);
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(10);
        env.getCheckpointConfig().enableExternalizedCheckpoints(
                CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        env.setStateBackend((StateBackend) new FsStateBackend("file:///tmp/flink/checkpints"));

        SingleOutputStreamOperator<Row> sourceStream = env
                .addSource(new UrlClickRowDataSource())
                .returns(USER_CLICK_TYPEINFO)
                .setParallelism(10)
                .name("Dummy Source");

        SingleOutputStreamOperator<Row> streamWithWatermark = sourceStream
                .assignTimestampsAndWatermarks(new AscendingTimestampExtractor<Row>() {
                    @Override
                    public long extractAscendingTimestamp(Row element) {
                        return Timestamp.valueOf(element.getField(3).toString()).getTime();
                    }
                });
        CacheStreamFunction cacheStreamFunction = new CacheStreamFunction("username", (RowTypeInfo) USER_CLICK_TYPEINFO);
        SingleOutputStreamOperator<Row> cacheStream = streamWithWatermark
                .keyBy(1)
                .process(cacheStreamFunction);

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
