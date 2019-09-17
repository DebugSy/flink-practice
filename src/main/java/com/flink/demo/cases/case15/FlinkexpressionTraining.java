package com.flink.demo.cases.case15;

import org.apache.calcite.config.Lex;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.impl.AbstractTable;
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
import org.apache.flink.table.expressions.EqualTo;
import org.apache.flink.table.expressions.Expression;
import org.apache.flink.table.expressions.Literal;
import org.apache.flink.table.expressions.ResolvedFieldReference;

/**
 * Created by P0007 on 2019/9/17.
 */
public class FlinkexpressionTraining {

    public static final SqlTypeFactoryImpl TYPE_FACTORY = new SqlTypeFactoryImpl(
            RelDataTypeSystem.DEFAULT);

    public static final RelDataTypeSystem TYPE_SYSTEM = RelDataTypeSystem.DEFAULT;

    public static void main(String[] args) {

        CalciteSchema internalSchema = CalciteSchema.createRootSchema(false, false);
        SchemaPlus rootSchema = internalSchema.plus();

        //添加表 test
        rootSchema.add("tableA", new AbstractTable() {
            @Override
            public RelDataType getRowType(RelDataTypeFactory typeFactory) {
                RelDataTypeFactory.Builder builder = new RelDataTypeFactory
                        .Builder(TYPE_FACTORY);
                //列id, 类型int
                builder.add("id", new BasicSqlType(TYPE_SYSTEM, SqlTypeName.INTEGER));
                //列name, 类型为varchar
                builder.add("name", new BasicSqlType(TYPE_SYSTEM, SqlTypeName.VARCHAR));
                builder.add("time_str", new BasicSqlType(TYPE_SYSTEM, SqlTypeName.VARCHAR));
                return builder.build();
            }
        });

        SqlParser.Config sqlParserConfig = SqlParser
                .configBuilder()
                .setLex(Lex.JAVA)
                .build();

        SqlToRelConverter.Config sqlToRelConfig = SqlToRelConverter.configBuilder()
                .withTrimUnusedFields(false)
                .withConvertTableAccess(false)
                .withInSubQueryThreshold(Integer.MAX_VALUE)
                .build();

        FrameworkConfig frameworkConfig = Frameworks
                .newConfigBuilder()
                .defaultSchema(rootSchema)
                .parserConfig(sqlParserConfig)
                .typeSystem(new FlinkTypeSystem())
                .sqlToRelConverterConfig(sqlToRelConfig)
                .executor(new ExpressionReducer(new TableConfig()))
                .build();

        FlinkRelBuilder relBuilder = FlinkRelBuilder.create(frameworkConfig);

//        Expression expression = ExpressionParser.parseExpression("name = 'UserA'");
        Expression pred2 = new EqualTo(
                new ResolvedFieldReference("name", Types.STRING),
                new Literal("UserA", Types.STRING));
        System.out.println(pred2);
        relBuilder.scan("tableA");
        RexNode rexNode = pred2.toRexNode(relBuilder);
        System.out.println(rexNode);
    }

}
