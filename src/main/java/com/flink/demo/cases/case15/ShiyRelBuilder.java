package com.flink.demo.cases.case15;

import org.apache.calcite.config.CalciteConnectionConfigImpl;
import org.apache.calcite.config.CalciteConnectionProperty;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.plan.*;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.rel.rules.TableScanRule;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.RelBuilder;

import java.util.Collections;
import java.util.Properties;

/**
 * Created by P0007 on 2019/9/19.
 */
public class ShiyRelBuilder extends RelBuilder {

    protected ShiyRelBuilder(Context context, RelOptCluster cluster, RelOptSchema relOptSchema) {
        super(context, cluster, relOptSchema);
    }

    public RelOptPlanner getPlanner() {
        RelOptPlanner relOptPlanner = cluster.getPlanner();
        return relOptPlanner;
    }


    /** Creates a RelBuilder. */
    public static ShiyRelBuilder create(FrameworkConfig config) {
        RelDataTypeSystem typeSystem = config.getTypeSystem();
        JavaTypeFactoryImpl typeFactory = new JavaTypeFactoryImpl(typeSystem);
        VolcanoPlanner planner = new VolcanoPlanner();
        planner.setExecutor(config.getExecutor());
        planner.addRelTraitDef(ConventionTraitDef.INSTANCE);

        RelOptCluster cluster = RelOptCluster.create(planner, new RexBuilder(typeFactory));
        CalciteSchema calciteSchema = CalciteSchema.from(config.getDefaultSchema());

        Properties properties = new Properties();
        SqlParser.Config parserConfig = config.getParserConfig();
        properties.setProperty(
                CalciteConnectionProperty.CASE_SENSITIVE.camelName(),
                String.valueOf(parserConfig.caseSensitive()));
        CalciteConnectionConfigImpl calciteConnectionConfig = new CalciteConnectionConfigImpl(properties);

        CalciteCatalogReader relOptSchema = new CalciteCatalogReader(
                calciteSchema,
                Collections.emptyList(),
                typeFactory,
                calciteConnectionConfig);
        return new ShiyRelBuilder(config.getContext(), cluster, relOptSchema);
    }



}
