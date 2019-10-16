package com.flink.demo.cases.case15;

import com.flink.demo.cases.common.datasource.OutOfOrderDataSource;
import com.flink.demo.cases.common.datasource.UrlClickRowDataSource;
import org.apache.calcite.plan.RelOptSchema;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.hep.HepMatchOrder;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexLocalRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexProgram;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.tools.RuleSet;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.api.Types;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.calcite.FlinkPlannerImpl;
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
import org.apache.flink.table.runtime.CRowOutputProcessRunner;
import org.apache.flink.table.runtime.types.CRowTypeInfo;
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
public class FlinkExpressionTraining7 {

    private static TypeInformation userClickTypeInfo = Types.ROW(
            new String[]{"userId", "username", "url", "clickTime"},
            new TypeInformation[]{
                    Types.INT(),
                    Types.STRING(),
                    Types.STRING(),
                    Types.SQL_TIMESTAMP()
            });

    private static TypeInformation resultTypeInfo = Types.ROW(
            new String[]{"username", "tag1"},
            new TypeInformation[]{
                    Types.STRING(),
                    Types.INT()
            });

    private static String hopWindowSql = "select username, count(*) as cnt, " +
            "HOP_ROWTIME(rowtime, INTERVAL '5' SECOND, INTERVAL '10' SECOND) as window_rowtime, " +
            "HOP_START(rowtime, INTERVAL '5' SECOND, INTERVAL '10' SECOND) as window_start, " +
            "HOP_END(rowtime, INTERVAL '5' SECOND, INTERVAL '10' SECOND) as window_end " +
            "from clicks " +
            "where url like '%/api/H%'" +
            "group by username, " +
            "HOP(rowtime, INTERVAL '5' SECOND, INTERVAL '10' SECOND)";

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        env.setParallelism(4);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        SingleOutputStreamOperator<Row> streamSource = env
                .addSource(new UrlClickRowDataSource())
                .returns(userClickTypeInfo).name("Dummy Url Click Source");

        SingleOutputStreamOperator<Row> operator = streamSource.assignTimestampsAndWatermarks(new AscendingTimestampExtractor<Row>() {
            @Override
            public long extractAscendingTimestamp(Row element) {
                return Timestamp.valueOf(element.getField(3).toString()).getTime();
            }
        });

        tEnv.registerDataStream("clicks", operator, OutOfOrderDataSource.CLICK_FIELDS);

        //处理表达式中的字段， 如: UnResolvedField -> ResolvedField
        FlinkRelBuilder relBuilder = tEnv.relBuilder();

        FlinkPlannerImpl planner = new FlinkPlannerImpl(tEnv.getFrameworkConfig(), tEnv.getPlanner(), tEnv.getTypeFactory());
        SqlNode sqlNode = planner.parse("select username, case when username like '%户A' then 1 else 0 end as tag1 " +
                " from clicks where url like '%http://www.inforefiner.com/api%' ");
        SqlNode validateSqlNode = planner.validate(sqlNode);
        RelRoot relRoot = planner.rel(validateSqlNode);
        RelNode relNode = relRoot.rel;
        RelDataType rowType = relNode.getRowType();

        RelNode dataStreamPlan = tEnv.optimize(relNode, true);

        DataStreamCalc datastreamCalc = (DataStreamCalc) dataStreamPlan;
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

        CodeGenProcessFunction codeGenProcessFunction = new CodeGenProcessFunction(
                myProcessFunction.name(),
                myProcessFunction.code(),
                userClickTypeInfo);

        SingleOutputStreamOperator my_filter = streamSource.process(codeGenProcessFunction)
                .name("My Filter")
                .returns(resultTypeInfo);
        my_filter.printToErr("Dummy Url Click Sink");

        env.execute("CodeGen Test");

    }

}
