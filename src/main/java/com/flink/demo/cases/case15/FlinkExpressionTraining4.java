package com.flink.demo.cases.case15;

import com.flink.demo.cases.common.datasource.OutOfOrderDataSource;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.rex.*;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.BasicSqlType;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql2rel.SqlToRelConverter;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Frameworks;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.api.Types;
import org.apache.flink.table.calcite.FlinkRelBuilder;
import org.apache.flink.table.calcite.FlinkTypeSystem;
import org.apache.flink.table.calcite.RelTimeIndicatorConverter;
import org.apache.flink.table.codegen.CodeGenerator;
import org.apache.flink.table.codegen.FunctionCodeGenerator;
import org.apache.flink.table.codegen.GeneratedExpression;
import org.apache.flink.table.codegen.GeneratedFunction;
import org.apache.flink.table.plan.nodes.logical.FlinkLogicalCalc;
import org.apache.flink.table.plan.schema.DataStreamTable;
import org.apache.flink.table.plan.schema.RowSchema;
import org.apache.flink.table.plan.stats.FlinkStatistic;
import org.apache.flink.types.Row;
import scala.Option;
import scala.collection.JavaConverters;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by P0007 on 2019/9/17.
 */
public class FlinkExpressionTraining4 {

    public static final SqlTypeFactoryImpl TYPE_FACTORY = new SqlTypeFactoryImpl(
            RelDataTypeSystem.DEFAULT);

    public static final RelDataTypeSystem TYPE_SYSTEM = RelDataTypeSystem.DEFAULT;

    private static TypeInformation userClickTypeInfo = Types.ROW(
            new String[]{"id", "name", "time_str"},
            new TypeInformation[]{
                    Types.STRING(),
                    Types.STRING(),
                    Types.SQL_TIMESTAMP()
            });

    private static String fields = "username,url,clickTime.rowtime";

    public static void main(String[] args) {

        CalciteSchema internalSchema = CalciteSchema.createRootSchema(false, false);
        SchemaPlus rootSchema = internalSchema.plus();

        RelDataTypeFactory.Builder builder = new RelDataTypeFactory
                .Builder(TYPE_FACTORY);
        builder.add("id", new BasicSqlType(TYPE_SYSTEM, SqlTypeName.INTEGER));
        builder.add("name", new BasicSqlType(TYPE_SYSTEM, SqlTypeName.VARCHAR));
        builder.add("time_str", new BasicSqlType(TYPE_SYSTEM, SqlTypeName.VARCHAR));

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        DataStreamSource<Tuple3<String, String, Timestamp>> sourceStream = env.addSource(new OutOfOrderDataSource());
        KeyedStream<Tuple3<String, String, Timestamp>, Tuple> keyedStream = sourceStream.assignTimestampsAndWatermarks(new AscendingTimestampExtractor<Tuple3<String, String, Timestamp>>() {
            @Override
            public long extractAscendingTimestamp(Tuple3<String, String, Timestamp> element) {
                return element.f2.getTime();
            }
        }).keyBy(0);

        DataStreamTable<Tuple3<String, String, Timestamp>> dataStreamTable = new DataStreamTable<>(keyedStream,
                new int[]{0, 1, -1},
                new String[]{"id", "name", "time_str"},
                FlinkStatistic.UNKNOWN());

        //添加表 test
        rootSchema.add("tableA", dataStreamTable);

        FrameworkConfig frameworkConfig = Frameworks
                .newConfigBuilder()
                .defaultSchema(rootSchema)
                .typeSystem(new FlinkTypeSystem())
                .build();

        FlinkRelBuilder relBuilder = FlinkRelBuilder.create(frameworkConfig);
        relBuilder.scan("tableA");

        RexBuilder rexBuilder = new RexBuilder(relBuilder.getTypeFactory());
        RexInputRef username = relBuilder.field("name");
        RexLiteral userA = rexBuilder.makeLiteral("userA");
        RexNode rexCall = relBuilder.call(SqlStdOperatorTable.EQUALS, username, userA);
        RelNode filter = relBuilder.filter(rexCall).build();
        LogicalFilter logicalFilter = (LogicalFilter) filter;
        RelNode rel = logicalFilter.getInput();
        RelDataType inputRowType = rel.getRowType();


        FlinkOptimizer optimizer = new FlinkOptimizer(relBuilder, frameworkConfig);
        RelNode optimizeRelNode = optimizer.optimize3(filter);
        System.out.println(RelOptUtil.toString(optimizeRelNode));

        FlinkLogicalCalc flinkLogicalCalc = (FlinkLogicalCalc) optimizeRelNode;
        RexProgram calcProgram = flinkLogicalCalc.getProgram();
        RexLocalRef condition = calcProgram.getCondition();
        RexNode convertExpression = RelTimeIndicatorConverter.convertExpression(
                calcProgram.expandLocalRef(condition),
                inputRowType,
                rexBuilder
        );

        System.out.println(convertExpression);

        List<RexLocalRef> projectList = calcProgram.getProjectList();
        List<RexNode> project = new ArrayList<>();
        for (RexLocalRef rexLocalRef : projectList) {
            RexNode rexNode = calcProgram.expandLocalRef(rexLocalRef);
            project.add(rexNode);
        }

        FunctionCodeGenerator generator = new FunctionCodeGenerator(
                new TableConfig(),
                false,
                userClickTypeInfo,
                null,
                null,
                null);

        RowSchema rowSchema = new RowSchema(inputRowType);
        GeneratedExpression projectionGen = generator.generateResultExpression(
                rowSchema.typeInfo(),
                rowSchema.fieldNames(),
                JavaConverters.asScalaIteratorConverter(project.iterator()).asScala().toSeq());
        System.out.println(projectionGen);

        GeneratedExpression filterCondition = generator.generateExpression(convertExpression);
        StringBuilder filterStr = new StringBuilder();
        StringBuilder bodyBuilder = filterStr.append(filterCondition.code()).append("\n")
                .append("if (").append(filterCondition.resultTerm()).append(") {").append("\n")
                .append(projectionGen.code()).append("\n")
                .append(generator.collectorTerm()).append(".collect(").append(projectionGen.resultTerm()).append(");").append("\n")
                .append("}");
        String body = bodyBuilder.toString();
        GeneratedFunction<ProcessFunction, Row> myProcessFunction = generator.generateFunction(
                "MyProcessFunction",
                ProcessFunction.class,
                body,
                rowSchema.typeInfo()
        );
        System.out.println(myProcessFunction.code());

    }

}
