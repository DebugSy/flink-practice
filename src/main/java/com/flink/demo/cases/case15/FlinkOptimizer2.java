package com.flink.demo.cases.case15;

import com.google.common.collect.ImmutableList;
import org.apache.calcite.adapter.enumerable.EnumerableConvention;
import org.apache.calcite.adapter.enumerable.EnumerableRules;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.plan.hep.HepMatchOrder;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgramBuilder;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.rules.FilterToCalcRule;
import org.apache.calcite.rel.rules.TableScanRule;
import org.apache.calcite.tools.*;
import org.apache.flink.table.calcite.FlinkRelBuilder;
import org.apache.flink.table.plan.nodes.FlinkConventions;
import org.apache.flink.table.plan.nodes.logical.*;
import org.apache.flink.table.plan.rules.FlinkRuleSets;

import java.util.Iterator;

/**
 * Created by P0007 on 2019/9/19.
 */
public class FlinkOptimizer2 {

    private ShiyRelBuilder shiyRrelBuilder;

    private FlinkRelBuilder flinkRelBuilder;

    public FlinkOptimizer2(ShiyRelBuilder shiyRrelBuilder) {
        this.shiyRrelBuilder = shiyRrelBuilder;
    }

    public FlinkOptimizer2(FlinkRelBuilder flinkRelBuilder) {
        this.flinkRelBuilder = flinkRelBuilder;
    }

    public RelNode optimize(RelNode relNode) {
        Program optProgram =  Programs.ofRules(ruleSet);
        RelTraitSet traitSet = relNode.getTraitSet().replace(FlinkConventions.LOGICAL()).simplify();
        RelNode resultRelNode = optProgram.run(flinkRelBuilder.getPlanner(), relNode, traitSet, ImmutableList.of(), ImmutableList.of());
        return resultRelNode;
    }

    public RelNode optimize3(RelNode relNode) {
        RelOptPlanner planner = flinkRelBuilder.getPlanner();
//        planner.addRule(EnumerableRules.ENUMERABLE_PROJECT_RULE);
        planner.clear();
        for (RelOptRule rule : ruleSet) {
            planner.addRule(rule);
        }
        RelTraitSet desiredTraits = relNode.getCluster().traitSet().replace(FlinkConventions.LOGICAL());
        relNode = planner.changeTraits(relNode, desiredTraits);
        planner.setRoot(relNode);
        relNode = planner.findBestExp();
        return relNode;
    }

    public RelNode optimize2(RelNode relNode) {
        RelOptPlanner planner = shiyRrelBuilder.getPlanner();
//        planner.addRule(EnumerableRules.ENUMERABLE_PROJECT_RULE);
        planner.addRule(EnumerableRules.ENUMERABLE_FILTER_RULE);
        planner.addRule(EnumerableRules.ENUMERABLE_TABLE_SCAN_RULE);
        planner.addRule(TableScanRule.INSTANCE);

        RelTraitSet desiredTraits = relNode.getCluster().traitSet()
                .replace(EnumerableConvention.INSTANCE);
        relNode = planner.changeTraits(relNode, desiredTraits);
        planner.setRoot(relNode);
        relNode = planner.findBestExp();
        return relNode;
    }

    RuleSet ruleSet = RuleSets.ofList(
            FilterToCalcRule.INSTANCE,

            FlinkLogicalCalc.CONVERTER(),
            FlinkLogicalCorrelate.CONVERTER(),
            FlinkLogicalIntersect.CONVERTER(),
            FlinkLogicalJoin.CONVERTER(),
            FlinkLogicalTemporalTableJoin.CONVERTER(),
            FlinkLogicalMinus.CONVERTER(),
            FlinkLogicalSort.CONVERTER(),
            FlinkLogicalUnion.CONVERTER(),
            FlinkLogicalValues.CONVERTER(),
            FlinkLogicalTableSourceScan.CONVERTER(),
            FlinkLogicalTableFunctionScan.CONVERTER(),
            FlinkLogicalNativeTableScan.CONVERTER(),
            FlinkLogicalMatch.CONVERTER()
  );

}
