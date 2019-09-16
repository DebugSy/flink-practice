package com.flink.demo.cases.case14;

import com.flink.demo.cases.common.datasource.UrlClickCRowDataSource;
import com.flink.demo.cases.common.datasource.UrlClickRowDataSource;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.api.Types;
import org.apache.flink.table.codegen.FunctionCodeGenerator;
import org.apache.flink.table.codegen.GeneratedFunction;
import org.apache.flink.table.runtime.CRowProcessRunner;
import org.apache.flink.table.runtime.types.CRow;
import org.apache.flink.table.runtime.types.CRowTypeInfo;
import org.apache.flink.types.Row;

/**
 * Created by P0007 on 2019/9/3.
 *
 * Flink 训练 - 代码生成
 *
 */
public class FlinkTraining_filter {

    private static TypeInformation userClickTypeInfo = Types.ROW(
            new String[]{"username", "url", "clickTime"},
            new TypeInformation[]{
                    Types.STRING(),
                    Types.STRING(),
                    Types.LONG()
            });

    private static String body = "\n" +
            "    \n" +
            "    boolean isNull$13 = (java.lang.Long) in1.getField(2) == null;\n" +
            "    long result$12;\n" +
            "    if (isNull$13) {\n" +
            "      result$12 = -1L;\n" +
            "    }\n" +
            "    else {\n" +
            "      result$12 = (long) org.apache.calcite.runtime.SqlFunctions.toLong((java.lang.Long) in1.getField(2));\n" +
            "    }\n" +
            "    \n" +
            "    \n" +
            "    boolean isNull$9 = (java.lang.String) in1.getField(0) == null;\n" +
            "    java.lang.String result$8;\n" +
            "    if (isNull$9) {\n" +
            "      result$8 = \"\";\n" +
            "    }\n" +
            "    else {\n" +
            "      result$8 = (java.lang.String) (java.lang.String) in1.getField(0);\n" +
            "    }\n" +
            "    \n" +
            "    \n" +
            "    boolean isNull$11 = (java.lang.String) in1.getField(1) == null;\n" +
            "    java.lang.String result$10;\n" +
            "    if (isNull$11) {\n" +
            "      result$10 = \"\";\n" +
            "    }\n" +
            "    else {\n" +
            "      result$10 = (java.lang.String) (java.lang.String) in1.getField(1);\n" +
            "    }\n" +
            "    \n" +
            "    \n" +
            "    \n" +
            "org.apache.flink.types.Row out =\n" +
            "      new org.apache.flink.types.Row(3);" +
            "    \n" +
            "    \n" +
            "    \n" +
            "    \n" +
            "    \n" +
            "    java.lang.String result$15 = \"userA\";\n" +
            "    \n" +
            "    boolean isNull$17 = isNull$9 || false;\n" +
            "    boolean result$16;\n" +
            "    if (isNull$17) {\n" +
            "      result$16 = false;\n" +
            "    }\n" +
            "    else {\n" +
            "      result$16 = result$8.compareTo(result$15) == 0;\n" +
            "    }\n" +
            "    \n" +
            "    if (result$16) {\n" +
            "      \n" +
            "    \n" +
            "    if (isNull$9) {\n" +
            "      out.setField(0, null);\n" +
            "    }\n" +
            "    else {\n" +
            "      out.setField(0, result$8);\n" +
            "    }\n" +
            "    \n" +
            "    \n" +
            "    \n" +
            "    if (isNull$11) {\n" +
            "      out.setField(1, null);\n" +
            "    }\n" +
            "    else {\n" +
            "      out.setField(1, result$10);\n" +
            "    }\n" +
            "    \n" +
            "    \n" +
            "    \n" +
            "    \n" +
            "    java.lang.Long result$14;\n" +
            "    if (isNull$13) {\n" +
            "      result$14 = null;\n" +
            "    }\n" +
            "    else {\n" +
            "      result$14 = result$12;\n" +
            "    }\n" +
            "    \n" +
            "    if (isNull$13) {\n" +
            "      out.setField(2, null);\n" +
            "    }\n" +
            "    else {\n" +
            "      out.setField(2, result$14);\n" +
            "    }\n" +
            "    \n" +
            "      c.collect(out);\n" +
            "    }";

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        DataStream<CRow> sourceStream = env
                .addSource(new UrlClickCRowDataSource())
                .returns(new CRowTypeInfo((RowTypeInfo) userClickTypeInfo));

        TableConfig tableConfig = new TableConfig();
        FunctionCodeGenerator generator = new FunctionCodeGenerator(
                tableConfig,
                false,
                userClickTypeInfo,
                null,
                null,
                null);

//        generator.generateResultExpression()

        GeneratedFunction<ProcessFunction, Row> filterFunction = generator.generateFunction(
                "FilterFunction",
                ProcessFunction.class,
                body,
                (RowTypeInfo) userClickTypeInfo);

        CRowProcessRunner procFunction = new CRowProcessRunner(
                filterFunction.name(),
                filterFunction.code(),
                new CRowTypeInfo((RowTypeInfo) userClickTypeInfo)
        );

        sourceStream.process(procFunction).printToErr();


        env.setParallelism(1);
        env.execute("Flink Format Training");

    }

}
