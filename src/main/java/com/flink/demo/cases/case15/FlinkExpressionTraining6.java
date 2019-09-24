package com.flink.demo.cases.case15;

import com.flink.demo.cases.common.datasource.OutOfOrderDataSource;
import com.flink.demo.cases.common.datasource.UrlClickRowDataSource;
import org.apache.calcite.plan.RelOptSchema;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.hep.HepMatchOrder;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexLocalRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexProgram;
import org.apache.calcite.tools.RuleSet;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.api.Types;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.calcite.FlinkRelBuilder;
import org.apache.flink.table.calcite.RelTimeIndicatorConverter;
import org.apache.flink.table.codegen.FunctionCodeGenerator;
import org.apache.flink.table.codegen.GeneratedExpression;
import org.apache.flink.table.codegen.GeneratedFunction;
import org.apache.flink.table.expressions.Expression;
import org.apache.flink.table.expressions.ExpressionParser;
import org.apache.flink.table.plan.logical.CatalogNode;
import org.apache.flink.table.plan.logical.Filter;
import org.apache.flink.table.plan.nodes.FlinkConventions;
import org.apache.flink.table.plan.nodes.datastream.DataStreamCalc;
import org.apache.flink.table.plan.schema.RowSchema;
import org.apache.flink.types.Row;
import scala.collection.JavaConverters;
import scala.collection.Seq;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Created by P0007 on 2019/9/17.
 */
public class FlinkExpressionTraining6 {

    private static TypeInformation userClickTypeInfo = Types.ROW(
            new String[]{"userId", "username", "url", "clickTime"},
            new TypeInformation[]{
                    Types.INT(),
                    Types.STRING(),
                    Types.STRING(),
                    Types.SQL_TIMESTAMP()
            });

    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        SingleOutputStreamOperator streamSource = env
                .addSource(new UrlClickRowDataSource())
                .returns(userClickTypeInfo);


        tEnv.registerDataStream("tableA", streamSource, OutOfOrderDataSource.CLICK_FIELDS);

        //原始表达式生成
        Expression filterExpr = ExpressionParser.parseExpression(" username == 'user A' && userId > 60  ");

        //处理表达式中的字段， 如: UnResolvedField -> ResolvedField
        FlinkRelBuilder relBuilder = tEnv.relBuilder();
        RelOptSchema relOptSchema = relBuilder.getRelOptSchema();
        RelOptTable relOptTable = relOptSchema.getTableForMember(Arrays.asList("tableA"));
        RelDataType rowType = relOptTable.getRowType();
        Seq<String> tablePath = JavaConverters.asScalaIteratorConverter(Arrays.asList("tableA").iterator()).asScala().toSeq();
        CatalogNode catalogNode = new CatalogNode(tablePath, rowType);

        Filter filter1 = new Filter(filterExpr, catalogNode);
        Filter resolvedLogicPlan = (Filter)filter1.validate(tEnv);
        RelNode toRelNode = resolvedLogicPlan.toRelNode(relBuilder);

        //逻辑计划优化阶段
        FlinkOptimizer2 optimizer = new FlinkOptimizer2(relBuilder);
        RelNode optimizeRelNode = optimizer.optimize3(toRelNode);

        //物理计划优化阶段
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

        //提取结果字段
        List<RexNode> project = new ArrayList<>();
        List<RexLocalRef> projectList = calcProgram.getProjectList();
        for (RexLocalRef rexLocalRef : projectList) {
            RexNode rexNode = calcProgram.expandLocalRef(rexLocalRef);
            project.add(rexNode);
        }

        //构造代码生成器
        FunctionCodeGenerator generator = new FunctionCodeGenerator(
                new TableConfig(),
                false,
                userClickTypeInfo,
                null,
                null,
                null);

        RowSchema rowSchema = new RowSchema(rowType);
        //生成结果字段
        GeneratedExpression projectionGen = generator.generateResultExpression(
                rowSchema.typeInfo(),
                rowSchema.fieldNames(),
                JavaConverters.asScalaIteratorConverter(project.iterator()).asScala().toSeq());

        //生成表达式
        RexNode convertExpression = RelTimeIndicatorConverter.convertExpression(
                calcProgram.expandLocalRef(calcProgram.getCondition()),
                rowType,
                datastreamCalc.getCluster().getRexBuilder()
        );
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
