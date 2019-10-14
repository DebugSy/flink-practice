package com.flink.demo.cases.case14;

import com.flink.demo.cases.common.datasource.UrlClickRowDataSource;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.serialization.TypeInformationSerializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.formats.csv.CsvRowSerializationSchema;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer010;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.api.Types;
import org.apache.flink.table.codegen.FunctionCodeGenerator;
import org.apache.flink.table.codegen.GeneratedFunction;
import org.apache.flink.table.runtime.CRowProcessRunner;
import org.apache.flink.table.runtime.types.CRowTypeInfo;
import org.apache.flink.types.Row;
import org.apache.kafka.clients.producer.ProducerConfig;

import java.util.Properties;

/**
 * Created by P0007 on 2019/9/3.
 *
 * Flink 训练 - 代码生成
 *
 */
public class FlinkTraining_generate_code {

    private static TypeInformation userClickTypeInfo = Types.ROW(
            new String[]{"userId", "username", "url", "clickTime"},
            new TypeInformation[]{
                    Types.INT(),
                    Types.STRING(),
                    Types.STRING(),
                    Types.SQL_TIMESTAMP()
            });

    private static String body = "\n" +
            "\n" +
            "\n" +
            "\n" +
            "java.lang.String result$15 = \"\\u7528\\u6237A\";\n" +
            "\n" +
            "boolean isNull$17 = isNull$9 || false;\n" +
            "boolean result$16;\n" +
            "if (isNull$17) {\n" +
            "  result$16 = false;\n" +
            "}\n" +
            "else {\n" +
            "  result$16 = result$8.compareTo(result$15) == 0;\n" +
            "}\n" +
            "\n" +
            "if (result$16) {\n" +
            "  \n" +
            "\n" +
            "if (isNull$9) {\n" +
            "  out.setField(0, null);\n" +
            "}\n" +
            "else {\n" +
            "  out.setField(0, result$8);\n" +
            "}\n" +
            "\n" +
            "\n" +
            "\n" +
            "if (isNull$11) {\n" +
            "  out.setField(1, null);\n" +
            "}\n" +
            "else {\n" +
            "  out.setField(1, result$10);\n" +
            "}\n" +
            "\n" +
            "\n" +
            "\n" +
            "\n" +
            "java.lang.Long result$14;\n" +
            "if (isNull$13) {\n" +
            "  result$14 = null;\n" +
            "}\n" +
            "else {\n" +
            "  result$14 = result$12;\n" +
            "}\n" +
            "\n" +
            "if (isNull$13) {\n" +
            "  out.setField(2, null);\n" +
            "}\n" +
            "else {\n" +
            "  out.setField(2, result$14);\n" +
            "}\n" +
            "\n" +
            "  c.collect(out);\n" +
            "}\n";

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        DataStream<Row> sourceStream = env
                .addSource(new UrlClickRowDataSource())
                .returns(userClickTypeInfo);
        TypeInformation<Row> rowTypeInfo = sourceStream.getType();

        TableConfig tableConfig = new TableConfig();
        FunctionCodeGenerator generator = new FunctionCodeGenerator(
                tableConfig,
                false,
                userClickTypeInfo,
                null,
                null,
                null);

        GeneratedFunction<ProcessFunction, Row> filterFunction = generator.generateFunction(
                "FilterFunction",
                ProcessFunction.class,
                body,
                (RowTypeInfo) userClickTypeInfo);

        CRowProcessRunner procFunction = new CRowProcessRunner(
                filterFunction.name(),
                filterFunction.code(),
                new CRowTypeInfo((RowTypeInfo) rowTypeInfo)
        );



        env.setParallelism(1);
        env.execute("Flink Format Training");

    }

}
