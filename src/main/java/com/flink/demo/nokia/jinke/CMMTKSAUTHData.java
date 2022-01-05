package com.flink.demo.nokia.jinke;

import org.apache.flink.types.Row;

import java.io.Serializable;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Random;
import java.util.UUID;

/**
 * Created by P0007 on 2020/3/4.
 */
public class CMMTKSAUTHData implements Serializable {

    private static final String SEPARATOR = ",";

    public String genData() {
        StringBuffer stringBuffer = new StringBuffer();
        UUID uuid = UUID.randomUUID();
        Random random = new Random(System.currentTimeMillis());
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyyMMdd");
        SimpleDateFormat timeFormat = new SimpleDateFormat("HHmmss");
        SimpleDateFormat allFormat = new SimpleDateFormat("yyyyMMddHHmmss");
        long currentTimeMillis = System.currentTimeMillis();
        Date date = new Date(currentTimeMillis);

        stringBuffer
                .append("FACEID-HMJ-" + uuid).append(SEPARATOR) //唯一流水号
                .append(random.nextDouble()).append(SEPARATOR) //分数
                .append(random.nextDouble()).append(SEPARATOR) // 误识率为千分之一的置信度阈值
                .append(random.nextDouble()).append(SEPARATOR) // 误识率为万分之一的置信度阈值
                .append(random.nextDouble()).append(SEPARATOR) // 误识率为十万分之一的置信度阈值
                .append(random.nextDouble()).append(SEPARATOR) // 误识率为百万分之一的置信度阈值
                .append(random.nextDouble()).append(SEPARATOR) // 误识率为百万分之一的置信度阈值
                .append("https://mca.cmpay.com:28710/fsgroup1/M00/C7/44"+random.nextLong()+".jpg").append(SEPARATOR) // 图片路径
                .append("Result description " + random.nextLong()).append(SEPARATOR) //结果描述
                .append(random.nextInt(3)).append(SEPARATOR) // 审核结果
                .append(random.nextLong()).append(SEPARATOR) //用户号
                .append(random.nextLong()).append(SEPARATOR) //手机号
                .append("username" + random.nextInt()).append(SEPARATOR) // 姓名
                .append(random.nextLong()).append(SEPARATOR) // 身份证号
                .append(dateFormat.format(date)).append(SEPARATOR) //审核日期
                .append(timeFormat.format(date)).append(SEPARATOR) //审核时间
                .append("response json").append(SEPARATOR) // 返回报文
                .append("1").append(SEPARATOR) // 认证类型
                .append("tm_smp data").append(SEPARATOR) // tm_smp
                .append(allFormat.format(date)).append(SEPARATOR) //请求ID
                .append("error message").append(SEPARATOR) //错误信息
                .append(random.nextLong()).append(SEPARATOR) // 旷视处理时间
                .append(random.nextLong()).append(SEPARATOR) //调用时间
                .append(random.nextInt(2)).append(SEPARATOR) //是否被攻击
                .append(random.nextInt(2)).append(SEPARATOR) //是否彩色照片
                .append("AUTH_BUS_TYP").append(SEPARATOR) // 业务类型
                .append(random.nextInt(10)).append(SEPARATOR) // 系统标识
                .append("https://mca.cmpay.com:28710/fsgroup1/"+random.nextLong()+".jpg").append(SEPARATOR) // 最佳人脸图片地址
                .append("https://mca.cmpay.com:28710/fsgroup1/"+random.nextLong()+".txt").append(SEPARATOR) // delta数据文件地址
                .append(random.nextDouble()).append(SEPARATOR) // 表示人脸照片为软件合成脸的置信度
                .append(random.nextDouble()).append(SEPARATOR) // 表示人脸照片为软件合成脸的置信度阈值
                .append(random.nextDouble()).append(SEPARATOR) // 表示人脸照片为面具的置信度
                .append(random.nextDouble()).append(SEPARATOR) // 表示人脸照片为面具的置信度阈值
                .append(random.nextDouble()).append(SEPARATOR) // 表示人脸照片为屏幕翻拍的置信度
                .append(random.nextDouble()).append(SEPARATOR) // 表示人脸照片为屏幕翻拍的置信度阈值
                .append(random.nextInt(2)).append(SEPARATOR) // 是否换脸攻击
                .append(System.currentTimeMillis()); // 时间戳
        return stringBuffer.toString();
    }

    public Row genRowData() {
        Row row = new Row(35);
        UUID uuid = UUID.randomUUID();
        Random random = new Random(System.currentTimeMillis());
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyyMMdd");
        SimpleDateFormat timeFormat = new SimpleDateFormat("HHmmss");
        SimpleDateFormat allFormat = new SimpleDateFormat("yyyyMMddHHmmss");
        long currentTimeMillis = System.currentTimeMillis();
        Date date = new Date(currentTimeMillis);

        row.setField(0, "FACEID-HMJ-" + uuid); //唯一流水号
        row.setField(1, String.valueOf(random.nextDouble())); //分数
        row.setField(2, random.nextDouble()); // 误识率为千分之一的置信度阈值
        row.setField(3, random.nextDouble()); // 误识率为万分之一的置信度阈值
        row.setField(4, random.nextDouble()); // 误识率为十万分之一的置信度阈值
        row.setField(5, random.nextDouble()); // 误识率为百万分之一的置信度阈值
        row.setField(6, "https://mca.cmpay.com:28710/fsgroup1/M00/C7/44"+random.nextLong()+".jpg"); // 图片路径
        row.setField(7, "Result description " + random.nextLong()); //结果描述
        row.setField(8, String.valueOf(random.nextInt(3))); // 审核结果
        row.setField(9, random.nextLong()); //用户号
        row.setField(10, random.nextLong()); //手机号
        row.setField(11, "username" + random.nextInt()); // 姓名
        row.setField(12, String.valueOf(random.nextLong())); // 身份证号
        row.setField(13, dateFormat.format(date)); //审核日期
        row.setField(14, timeFormat.format(date)); //审核时间
        row.setField(15, "response json"); // 返回报文
        row.setField(16, "1"); // 认证类型
        row.setField(17, new Timestamp(System.currentTimeMillis()).getTime()); // 时间戳
        row.setField(18, allFormat.format(date)); //请求ID
        row.setField(19, "error message");  //错误信息
        row.setField(20, String.valueOf(random.nextLong())); // 旷视处理时间
        row.setField(21, String.valueOf(random.nextLong())); //调用时间
        row.setField(22, String.valueOf(random.nextInt(2))); //是否被攻击
        row.setField(23, String.valueOf(random.nextInt(2))); //是否彩色照片
        row.setField(24, "AUTH_BUS_TYP"); // 业务类型
        row.setField(25, String.valueOf(random.nextInt(10))); // 系统标识
        row.setField(26, "https://mca.cmpay.com:28710/fsgroup1/"+random.nextLong()+".jpg"); // 最佳人脸图片地址
        row.setField(27, "https://mca.cmpay.com:28710/fsgroup1/"+random.nextLong()+".txt"); // delta数据文件地址
        row.setField(28, random.nextDouble());  // 表示人脸照片为软件合成脸的置信度
        row.setField(29, random.nextDouble());  // 表示人脸照片为软件合成脸的置信度阈值
        row.setField(30, random.nextDouble()); // 表示人脸照片为面具的置信度
        row.setField(31, random.nextDouble()); // 表示人脸照片为面具的置信度阈值
        row.setField(32, random.nextDouble()); // 表示人脸照片为屏幕翻拍的置信度
        row.setField(33, random.nextDouble()); // 表示人脸照片为屏幕翻拍的置信度阈值
        row.setField(34, String.valueOf(random.nextInt(2))); // 是否换脸攻击

        return row;
    }

}
