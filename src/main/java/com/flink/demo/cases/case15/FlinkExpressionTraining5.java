package com.flink.demo.cases.case15;

import com.flink.demo.cases.common.datasource.OutOfOrderDataSource;
import org.apache.calcite.plan.hep.HepMatchOrder;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.rules.FilterToCalcRule;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.rel.type.RelRecordType;
import org.apache.calcite.rex.*;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.RuleSet;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.table.api.*;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.calcite.FlinkRelBuilder;
import org.apache.flink.table.calcite.FlinkTypeSystem;
import org.apache.flink.table.calcite.RelTimeIndicatorConverter;
import org.apache.flink.table.codegen.FunctionCodeGenerator;
import org.apache.flink.table.codegen.GeneratedExpression;
import org.apache.flink.table.codegen.GeneratedFunction;
import org.apache.flink.table.expressions.Expression;
import org.apache.flink.table.expressions.ExpressionParser;
import org.apache.flink.table.plan.nodes.FlinkConventions;
import org.apache.flink.table.plan.nodes.datastream.DataStreamCalc;
import org.apache.flink.table.plan.nodes.datastream.DataStreamRel;
import org.apache.flink.table.plan.nodes.logical.FlinkLogicalCalc;
import org.apache.flink.table.plan.schema.DataStreamTable;
import org.apache.flink.table.plan.schema.RowSchema;
import org.apache.flink.table.plan.stats.FlinkStatistic;
import org.apache.flink.types.Row;
import scala.collection.JavaConverters;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by P0007 on 2019/9/17.
 */
public class FlinkExpressionTraining5 {

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
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        DataStreamSource<Tuple4<Integer, String, String, Timestamp>> sourceStream = env.addSource(new OutOfOrderDataSource());
        KeyedStream<Tuple4<Integer, String, String, Timestamp>, Tuple> keyedStream = sourceStream.assignTimestampsAndWatermarks(
                new AscendingTimestampExtractor<Tuple4<Integer, String, String, Timestamp>>() {
            @Override
            public long extractAscendingTimestamp(Tuple4<Integer, String, String, Timestamp> element) {
                return element.f3.getTime();
            }
        }).keyBy(0);

        tEnv.registerDataStream("tableA", keyedStream, OutOfOrderDataSource.CLICK_FIELDS);
        Table tableA = tEnv.scan("tableA");


        FlinkRelBuilder relBuilder = tEnv.relBuilder();

        Expression filterExpr = ExpressionParser.parseExpression(" username == 'user A' ");
        RexNode filterRexNode = filterExpr.toRexNode(relBuilder);
        RelNode filter = relBuilder.filter(filterRexNode).build();

        LogicalFilter logicalFilter = (LogicalFilter) filter;
        RelNode input = logicalFilter.getInput();
        RelDataType inputRowType = input.getRowType();


        FlinkOptimizer2 optimizer = new FlinkOptimizer2(relBuilder);
        RelNode optimizeRelNode = optimizer.optimize3(filter);

        RelNode physicalPlan = tEnv.optimizePhysicalPlan(optimizeRelNode, FlinkConventions.DATASTREAM());
        RuleSet decoRuleSet = tEnv.getDecoRuleSet();
        RelNode finalRelNode = tEnv.runHepPlannerSequentially(
                HepMatchOrder.BOTTOM_UP,
                decoRuleSet,
                physicalPlan,
                physicalPlan.getTraitSet()
        );

        DataStreamCalc datastreamCalc = (DataStreamCalc) finalRelNode;
        RexProgram calcProgram = datastreamCalc.getProgram();


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
        //生成结果字段
        GeneratedExpression projectionGen = generator.generateResultExpression(
                rowSchema.typeInfo(),
                rowSchema.fieldNames(),
                JavaConverters.asScalaIteratorConverter(project.iterator()).asScala().toSeq());

        RexNode convertExpression = RelTimeIndicatorConverter.convertExpression(
                calcProgram.expandLocalRef(calcProgram.getCondition()),
                inputRowType,
                datastreamCalc.getCluster().getRexBuilder()
        );

        //生成表达式
        GeneratedExpression filterCondition = generator.generateExpression(convertExpression);

        //生成processFunction的body
        StringBuilder bodyStr = new StringBuilder();
        StringBuilder bodyBuilder = bodyStr.append(filterCondition.code()).append("\n")
                .append("if (").append(filterCondition.resultTerm()).append(") {").append("\n")
                .append(projectionGen.code()).append("\n")
                .append(generator.collectorTerm()).append(".collect(").append(projectionGen.resultTerm()).append(");").append("\n")
                .append("}");
        String body = bodyBuilder.toString();

        //生成整个processFunction方法
        GeneratedFunction<ProcessFunction, Row> myProcessFunction = generator.generateFunction(
                "MyProcessFunction",
                ProcessFunction.class,
                body,
                rowSchema.typeInfo()
        );
        System.out.println(myProcessFunction.code());

    }

}
