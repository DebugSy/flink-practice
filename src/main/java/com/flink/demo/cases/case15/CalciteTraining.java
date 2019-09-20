package com.flink.demo.cases.case15;

import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.type.BasicSqlType;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.tools.*;

import static org.apache.calcite.sql.SqlExplainLevel.ALL_ATTRIBUTES;

/**
 * Created by P0007 on 2019/9/17.
 */
public class CalciteTraining {

    public static final SqlTypeFactoryImpl TYPE_FACTORY = new SqlTypeFactoryImpl(
            RelDataTypeSystem.DEFAULT);

    public static final RelDataTypeSystem TYPE_SYSTEM = RelDataTypeSystem.DEFAULT;


    public static void main(String[] args) throws SqlParseException, ValidationException, RelConversionException {

        CalciteSchema rootSchema = CalciteSchema
                .createRootSchema(false, false);

        //添加表 test
        rootSchema.add("test", new AbstractTable() {
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

        SqlParser.ConfigBuilder builder = SqlParser.configBuilder();
        builder.setCaseSensitive(false);

        FrameworkConfig config = Frameworks.newConfigBuilder()
                .defaultSchema(rootSchema.plus())
                .parserConfig(builder.build())
                .build();

        Planner planner = Frameworks.getPlanner(config);
        SqlNode originSqlNode = planner.parse("select * from test where id > 10");
        SqlNode sqlNode = planner.validate(originSqlNode);
        RelRoot relRoot = planner.rel(sqlNode);
        System.out.println(RelOptUtil.toString(relRoot.rel, ALL_ATTRIBUTES));
    }

}
