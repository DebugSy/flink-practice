package com.flink.demo.cases.case15;

import org.apache.calcite.config.Lex;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
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
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.calcite.FlinkRelBuilder;
import org.apache.flink.table.calcite.FlinkTypeSystem;
import org.apache.flink.table.codegen.ExpressionReducer;
import org.apache.flink.table.codegen.GeneratedExpression;
import org.apache.flink.table.expressions.EqualTo;
import org.apache.flink.table.expressions.Expression;
import org.apache.flink.table.expressions.Literal;
import org.apache.flink.table.expressions.ResolvedFieldReference;

/**
 * Created by P0007 on 2019/9/17.
 */
public class FlinkExpressionTraining2 {

    public static final SqlTypeFactoryImpl TYPE_FACTORY = new SqlTypeFactoryImpl(
            RelDataTypeSystem.DEFAULT);

    public static final RelDataTypeSystem TYPE_SYSTEM = RelDataTypeSystem.DEFAULT;

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

        AbstractTable table = new AbstractTable() {
            @Override
            public RelDataType getRowType(RelDataTypeFactory typeFactory) {
                return relDataType;
            }
        };

        //添加表 test
        rootSchema.add("tableA", table);

        FrameworkConfig frameworkConfig = Frameworks
                .newConfigBuilder()
                .defaultSchema(rootSchema)
                .parserConfig(SqlParser.Config.DEFAULT)
                .build();

//        FlinkRelBuilder relBuilder = FlinkRelBuilder.create(frameworkConfig);
        ShiyRelBuilder relBuilder = ShiyRelBuilder.create(frameworkConfig);
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

        FlinkOptimizer optimizer = new FlinkOptimizer(relBuilder, frameworkConfig);
        RelNode optimizeRelNode = optimizer.optimize2(filter);
        System.out.println(RelOptUtil.toString(optimizeRelNode));
    }

}
