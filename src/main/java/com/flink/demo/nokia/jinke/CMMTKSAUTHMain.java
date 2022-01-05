package com.flink.demo.nokia.jinke;

import com.flink.demo.nokia.jinke.udf.SubstringLastFunction;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import java.sql.Timestamp;

/**
 * Created by P0007 on 2020/3/4.
 */
public class CMMTKSAUTHMain {

    public static String fields = "HR_JRN_NO,SCORE,THOU_VTH,TEN_THOU_VTH,HUN_THOU_VTH," +
            "MILLION_VTH,PIC_URL,PRV_RMK,CHK_FLG,USR_NO,MBL_NO,CUS_NM,CTR_NO,CERT_VALID_DATE," +
            "CERT_VALID_TIME,DATA,AUTH_TYP,TM_SMP.rowtime,REQUEST_ID,ERR_MSG,USED_TM,REQ_TM,ATTA_FLG," +
            "MONO_FLG,AUTH_BUS_TYP,PLAT,FACE_URL,DELTA,SYN_FACE_CFDC,SYN_FACE_THR,MASK_CFDC," +
            "MASK_THR,SCRN_REP_CFDC,SCRN_REP_THR,FACE_REP";

    public static String sql = "SELECT\n" +
            "  SUM(REQ_NUM) AS SUM_REQ_NUM, -- 人脸比对的总次数\n" +
            "  SUM(SUC_NUM) AS SUM_SUC_NUM, -- 人脸比对的成功次数\n" +
            "  SUM(SUC_NUM) AS SUM_SUC_NUM, -- 人脸比对的失败次数\n" +
            "  SUM(FACE_MAT_SCORE) AS SUC_FACE_MAT_SCORE, -- 人脸比对分数达到阈值的次数\n" +
            "  SUM(USED_TM_CAST) AS SUM_USED_TM, -- 处理时长\n" +
            "  SUM(SYN_FACE_CFDC_NUM) AS SUM_SYN_FACE_CFDC, -- 软件合成脸次数\n" +
            "  SUM(MASK_CFDC_NUM) AS SUM_MASK_CFDC, -- 面具脸次数\n" +
            "  SUM(SCRN_REP_CFDC_NUM) AS SUM_SCRN_REP_CFDC, -- 屏幕翻拍次数\n" +
            "  SUM(FACE_REP_NUM) AS SUM_FACE_REP, -- 换脸攻击次数\n" +
            "  MINUTE(TUMBLE_START(TM_SMP, INTERVAL '10' SECOND)), -- 分钟数\n" +
            "  TUMBLE_START(TM_SMP, INTERVAL '10' SECOND), -- 窗口开始时间\n" +
            "  TUMBLE_ROWTIME(TM_SMP, INTERVAL '10' SECOND), -- 窗口最大时间\n" +
            "  TUMBLE_END(TM_SMP, INTERVAL '10' SECOND) -- 窗口结束时间\n" +
            "FROM\n" +
            "\t(\n" +
            "\t\tSELECT\n" +
            "\t\t\tHR_JRN_NO,\n" +
            "\t\t\tSCORE,\n" +
            "\t\t\tTHOU_VTH,\n" +
            "\t\t\tTEN_THOU_VTH,\n" +
            "\t\t\tHUN_THOU_VTH,\n" +
            "\t\t\tMILLION_VTH,\n" +
            "\t\t\tPIC_URL,\n" +
            "\t\t\tPRV_RMK,\n" +
            "\t\t\tCHK_FLG,\n" +
            "\t\t\tUSR_NO,\n" +
            "\t\t\tMBL_NO,\n" +
            "\t\t\tCUS_NM,\n" +
            "\t\t\tCTR_NO,\n" +
            "\t\t\tCERT_VALID_DATE,\n" +
            "\t\t\tCERT_VALID_TIME,\n" +
            "\t\t\tDATA,\n" +
            "\t\t\tAUTH_TYP,\n" +
            "\t\t\tTM_SMP,\n" +
            "\t\t\tREQUEST_ID,\n" +
            "\t\t\tERR_MSG,\n" +
            "\t\t\tUSED_TM,\n" +
            "\t\t\tREQ_TM,\n" +
            "\t\t\tATTA_FLG,\n" +
            "\t\t\tMONO_FLG,\n" +
            "\t\t\tAUTH_BUS_TYP,\n" +
            "\t\t\tPLAT,\n" +
            "\t\t\tFACE_URL,\n" +
            "\t\t\tDELTA,\n" +
            "\t\t\tSYN_FACE_CFDC,\n" +
            "\t\t\tSYN_FACE_THR,\n" +
            "\t\t\tMASK_CFDC,\n" +
            "\t\t\tMASK_THR,\n" +
            "\t\t\tSCRN_REP_CFDC,\n" +
            "\t\t\tSCRN_REP_THR,\n" +
            "\t\t\tFACE_REP,\n" +
            "\t\t\tRPOSITION(HR_JRN_NO, '-') AS HR_JRN_NO_PRE,\n"+
            "\t\t\tCAST(USED_TM AS BIGINT) AS USED_TM_CAST," +
            "\t\t\tCASE WHEN (CHK_FLG = '1' OR (CHK_FLG = '0' AND PRV_RMK = '身份信息与人脸不匹配，请核对后重试')) THEN 1 ELSE\t0\tEND AS SUC_NUM, -- 成功次数\n" +
            "\t\t\t1 AS REQ_NUM, -- 请求次数\n" +
            "\t\t\tCASE WHEN (SCORE >= TEN_THOU_VTH OR SCORE >= HUN_THOU_VTH) THEN 1 ELSE 0 END AS FACE_MAT_SCORE, -- 人脸比对分数达到阈值\n" +
            "\t\t\tCASE WHEN SYN_FACE_CFDC > SYN_FACE_THR THEN 1 ELSE 0 END AS SYN_FACE_CFDC_NUM, -- 软件合成脸次数\n" +
            "\t\t\tCASE WHEN MASK_CFDC > MASK_THR THEN 1 ELSE 0 END AS MASK_CFDC_NUM, -- 面具脸次数\n" +
            "\t\t\tCASE WHEN SCRN_REP_CFDC > SCRN_REP_THR THEN 1 ELSE 0 END AS SCRN_REP_CFDC_NUM, -- 屏幕翻拍次数\n" +
            "\t\t\tCASE WHEN FACE_REP = '1' THEN 1 ELSE 0 END AS FACE_REP_NUM -- 换脸攻击次数\n" +
            "\t\tFROM\n" +
            "\t\t\tCMMTKSAUTH\n" +
            "\t)\n" +
            "GROUP BY\n" +
            "\tUSR_NO,\n" +
            "\tAUTH_TYP,\n" +
            "\tCHK_FLG,\n" +
            "\tPRV_RMK,\n" +
            "\tAUTH_BUS_TYP,\n" +
            "\tPLAT,\n" +
            "\tHR_JRN_NO_PRE,\n" +
            "\tTUMBLE(TM_SMP, INTERVAL '10' SECOND)";

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        SingleOutputStreamOperator<Row> source = env.addSource(new CMMTKSAUTHStreamSource())
                .returns(CMMTKSAUTHStreamSource.CMMTKSAUTH_TYPEINFO)
                .name("source");

        SingleOutputStreamOperator<Row> streamWithWaterMark = source.assignTimestampsAndWatermarks(new AscendingTimestampExtractor<Row>() {
            @Override
            public long extractAscendingTimestamp(Row row) {
                return Long.valueOf(row.getField(17).toString());
            }
        });

        tableEnv.registerDataStream("CMMTKSAUTH", streamWithWaterMark, fields);

        tableEnv.registerFunction("RPOSITION", new SubstringLastFunction());
        Table table = tableEnv.sqlQuery(sql);

        DataStream<Row> sinkStream = tableEnv.toAppendStream(table, Row.class);
        sinkStream.printToErr();

        env.execute("CMMTKSAUTH JOB");
    }

}
