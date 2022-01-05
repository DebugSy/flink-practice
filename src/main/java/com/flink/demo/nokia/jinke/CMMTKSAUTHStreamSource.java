package com.flink.demo.nokia.jinke;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.table.api.Types;
import org.apache.flink.types.Row;

import java.util.Random;

/**
 * Created by P0007 on 2020/3/4.
 */
public class CMMTKSAUTHStreamSource extends RichSourceFunction<Row> {
    
    private volatile boolean running = true;
    
    private CMMTKSAUTHData dataGenerator = new CMMTKSAUTHData();
    
    @Override
    public void run(SourceContext<Row> ctx) throws Exception {
        while (running) {
            Random random = new Random(System.currentTimeMillis());
            Row row = dataGenerator.genRowData();
            ctx.collect(row);
            Thread.sleep(1000 * random.nextInt(5));
        }
    }

    @Override
    public void cancel() {
        running = false;
    }

    public static TypeInformation CMMTKSAUTH_TYPEINFO = Types.ROW(
            new String[]{
                    "HR_JRN_NO", //唯一流水号
                    "SCORE",//分数
                    "THOU_VTH",// 误识率为千分之一的置信度阈值
                    "TEN_THOU_VTH",// 误识率为万分之一的置信度阈值
                    "HUN_THOU_VTH",// 误识率为十万分之一的置信度阈值
                    "MILLION_VTH",// 误识率为百万分之一的置信度阈值
                    "PIC_URL",// 图片路径
                    "PRV_RMK",//结果描述
                    "CHK_FLG",// 审核结果
                    "USR_NO",//用户号
                    "MBL_NO",//手机号
                    "CUS_NM",// 姓名
                    "CTR_NO",// 身份证号
                    "CERT_VALID_DATE",//审核日期
                    "CERT_VALID_TIME",//审核时间
                    "DATA",// 返回报文
                    "AUTH_TYP",// 认证类型
                    "TM_SMP",// 时间戳
                    "REQUEST_ID",//请求ID
                    "ERR_MSG",//错误信息
                    "USED_TM",// 旷视处理时间
                    "REQ_TM",//调用时间
                    "ATTA_FLG",//是否被攻击
                    "MONO_FLG",//是否彩色照片
                    "AUTH_BUS_TYP",// 业务类型
                    "PLAT",// 系统标识
                    "FACE_URL",// 最佳人脸图片地址
                    "DELTA",// delta数据文件地址
                    "SYN_FACE_CFDC",// 表示人脸照片为软件合成脸的置信度
                    "SYN_FACE_THR",// 表示人脸照片为软件合成脸的置信度阈值
                    "MASK_CFDC",// 表示人脸照片为面具的置信度
                    "MASK_THR",// 表示人脸照片为面具的置信度阈值
                    "SCRN_REP_CFDC",// 表示人脸照片为屏幕翻拍的置信度
                    "SCRN_REP_THR",// 表示人脸照片为屏幕翻拍的置信度阈值
                    "FACE_REP"// 是否换脸攻击
            },
            new TypeInformation[]{
                    Types.STRING(),//唯一流水号
                    Types.STRING(),//分数
                    Types.DOUBLE(),// 误识率为千分之一的置信度阈值
                    Types.DOUBLE(),// 误识率为万分之一的置信度阈值
                    Types.DOUBLE(),// 误识率为十万分之一的置信度阈值
                    Types.DOUBLE(),// 误识率为百万分之一的置信度阈值
                    Types.STRING(),// 图片路径
                    Types.STRING(),//结果描述
                    Types.STRING(),// 审核结果
                    Types.LONG(),//用户号
                    Types.LONG(),//手机号
                    Types.STRING(),// 姓名
                    Types.STRING(),// 身份证号
                    Types.STRING(),//审核日期
                    Types.STRING(),//审核时间
                    Types.STRING(),// 返回报文
                    Types.STRING(),// 认证类型
                    Types.LONG(),// 时间戳
                    Types.STRING(),//请求ID
                    Types.STRING(),//错误信息
                    Types.STRING(),// 旷视处理时间
                    Types.STRING(),//调用时间
                    Types.STRING(),//是否被攻击
                    Types.STRING(),//是否彩色照片
                    Types.STRING(),// 业务类型
                    Types.STRING(),// 系统标识
                    Types.STRING(),// 最佳人脸图片地址
                    Types.STRING(),// delta数据文件地址
                    Types.DOUBLE(),// 表示人脸照片为软件合成脸的置信度
                    Types.DOUBLE(),// 表示人脸照片为软件合成脸的置信度阈值
                    Types.DOUBLE(),// 表示人脸照片为面具的置信度
                    Types.DOUBLE(),// 表示人脸照片为面具的置信度阈值
                    Types.DOUBLE(),// 表示人脸照片为屏幕翻拍的置信度
                    Types.DOUBLE(),// 表示人脸照片为屏幕翻拍的置信度阈值
                    Types.STRING()// 是否换脸攻击
            });
}
