package com.flink.demo.cases.case15;

import com.flink.demo.cases.common.datasource.OutOfOrderDataSource;
import org.apache.calcite.config.Lex;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.rex.*;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.type.BasicSqlType;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql2rel.SqlToRelConverter;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Frameworks;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.calcite.FlinkRelBuilder;
import org.apache.flink.table.calcite.FlinkTypeSystem;
import org.apache.flink.table.codegen.ExpressionReducer;
import org.apache.flink.table.plan.cost.DataSetCostFactory;
import org.apache.flink.table.plan.schema.DataStreamTable;
import org.apache.flink.table.plan.stats.FlinkStatistic;

import java.sql.Timestamp;
import java.util.Arrays;

/**
 * Created by P0007 on 2019/9/17.
 */
public class FlinkExpressionTraining3 {

    public static final SqlTypeFactoryImpl TYPE_FACTORY = new SqlTypeFactoryImpl(
            RelDataTypeSystem.DEFAULT);

    public static final RelDataTypeSystem TYPE_SYSTEM = RelDataTypeSystem.DEFAULT;

    private static String fields = "username,url,clickTime.rowtime";

    public static void main(String[] args) {

        CalciteSchema internalSchema = CalciteSchema.createRootSchema(false, false);
        SchemaPlus rootSchema = internalSchema.plus();

        RelDataTypeFactory.Builder builder = new RelDataTypeFactory
                .Builder(TYPE_FACTORY);
        //列id, 类型int
        builder.add("id", new BasicSqlType(TYPE_SYSTEM, SqlTypeName.INTEGER));
        //列name, 类型为varchar
        builder.add("name", new BasicSqlType(TYPE_SYSTEM, SqlTypeName.VARCHAR));
        builder.add("time_str", new BasicSqlType(TYPE_SYSTEM, SqlTypeName.VARCHAR));
        RelDataType relDataType = builder.build();

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
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

        DataStreamTable<Tuple4<Integer, String, String, Timestamp>> dataStreamTable = new DataStreamTable<>(keyedStream,
                new int[]{0, 1, -1},
                new String[]{"id", "name", "time_str"},
                FlinkStatistic.UNKNOWN());

        //添加表 test
        rootSchema.add("tableA", dataStreamTable);

        SqlToRelConverter.Config config = SqlToRelConverter.configBuilder()
                .withTrimUnusedFields(false)
                .withConvertTableAccess(false)
                .withInSubQueryThreshold(Integer.MAX_VALUE)
                .build();

        FrameworkConfig frameworkConfig = Frameworks
                .newConfigBuilder()
                .defaultSchema(rootSchema)
                .typeSystem(new FlinkTypeSystem())
                .build();

//        FlinkRelBuilder relBuilder = FlinkRelBuilder.create(frameworkConfig);
        FlinkRelBuilder relBuilder = FlinkRelBuilder.create(frameworkConfig);
        relBuilder.scan("tableA");

//        Expression expression = ExpressionParser.parseExpression("name = 'UserA'");
//        Expression pred2 = new EqualTo(
//                new ResolvedFieldReference("name", Types.STRING),
//                new Literal("UserA", Types.STRING));
//
//        RexNode rexNode = pred2.toRexNode(relBuilder);

        RexBuilder rexBuilder = new RexBuilder(relBuilder.getTypeFactory());
        RexInputRef username = relBuilder.field("name");
        RexLiteral userA = rexBuilder.makeLiteral("userA");
        RexNode rexCall = relBuilder.call(SqlStdOperatorTable.EQUALS, username, userA);
        RelNode filter = relBuilder.filter(rexCall).build();
        LogicalFilter logicalFilter = (LogicalFilter) filter;
        RelNode rel = logicalFilter.getInput();
        RelDataType inputRowType = rel.getRowType();
        RexProgramBuilder programBuilder = new RexProgramBuilder(inputRowType, rexBuilder);
        programBuilder.addIdentity();
        programBuilder.addCondition(logicalFilter.getCondition());
        RexProgram program = programBuilder.getProgram();


        FlinkOptimizer optimizer = new FlinkOptimizer(relBuilder, frameworkConfig);
        RelNode optimizeRelNode = optimizer.optimize3(filter);
        System.out.println(RelOptUtil.toString(optimizeRelNode));
    }

}
