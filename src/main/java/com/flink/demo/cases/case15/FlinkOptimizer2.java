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
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.rules.*;
import org.apache.calcite.tools.*;
import org.apache.flink.table.calcite.FlinkRelBuilder;
import org.apache.flink.table.plan.nodes.FlinkConventions;
import org.apache.flink.table.plan.nodes.logical.*;
import org.apache.flink.table.plan.rules.FlinkRuleSets;
import org.apache.flink.table.plan.rules.common.WindowAggregateReduceFunctionsRule;
import org.apache.flink.table.plan.rules.logical.LogicalUnnestRule;
import org.apache.flink.table.plan.rules.logical.PushFilterIntoTableSourceScanRule;
import org.apache.flink.table.plan.rules.logical.PushProjectIntoTableSourceScanRule;

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
        for (RelOptRule rule : relOptRules) {
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


    RuleSet relOptRules = RuleSets.ofList(

    // push a filter into a join
    FilterJoinRule.FILTER_ON_JOIN,
    // push filter into the children of a join
    FilterJoinRule.JOIN,
    // push filter through an aggregation
    FilterAggregateTransposeRule.INSTANCE,
    // push filter through set operation
    FilterSetOpTransposeRule.INSTANCE,
    // push project through set operation
    ProjectSetOpTransposeRule.INSTANCE,

    // aggregation and projection rules
    AggregateProjectMergeRule.INSTANCE,
    AggregateProjectPullUpConstantsRule.INSTANCE,
    // push a projection past a filter or vice versa
    ProjectFilterTransposeRule.INSTANCE,
    FilterProjectTransposeRule.INSTANCE,
            // push a projection to the children of a join
            // push all expressions to handle the time indicator correctly
            new ProjectJoinTransposeRule(PushProjector.ExprCondition.FALSE, RelFactories.LOGICAL_BUILDER),
    // merge projections
    ProjectMergeRule.INSTANCE,
    // remove identity project
    ProjectRemoveRule.INSTANCE,
    // reorder sort and projection
    SortProjectTransposeRule.INSTANCE,
    ProjectSortTransposeRule.INSTANCE,

    // join rules
    JoinPushExpressionsRule.INSTANCE,

    // remove union with only a single child
    UnionEliminatorRule.INSTANCE,
    // convert non-all union into all-union + distinct
    UnionToDistinctRule.INSTANCE,

    // remove aggregation if it does not aggregate and input is already distinct
    AggregateRemoveRule.INSTANCE,
    // push aggregate through join
    AggregateJoinTransposeRule.EXTENDED,
    // aggregate union rule
    AggregateUnionAggregateRule.INSTANCE,

    // reduce aggregate functions like AVG, STDDEV_POP etc.
    AggregateReduceFunctionsRule.INSTANCE,
    WindowAggregateReduceFunctionsRule.INSTANCE,

    // remove unnecessary sort rule
    SortRemoveRule.INSTANCE,

    // prune empty results rules
    PruneEmptyRules.AGGREGATE_INSTANCE,
    PruneEmptyRules.FILTER_INSTANCE,
    PruneEmptyRules.JOIN_LEFT_INSTANCE,
    PruneEmptyRules.JOIN_RIGHT_INSTANCE,
    PruneEmptyRules.PROJECT_INSTANCE,
    PruneEmptyRules.SORT_INSTANCE,
    PruneEmptyRules.UNION_INSTANCE,

    // calc rules
    FilterCalcMergeRule.INSTANCE,
    ProjectCalcMergeRule.INSTANCE,
    FilterToCalcRule.INSTANCE,
    ProjectToCalcRule.INSTANCE,
    CalcMergeRule.INSTANCE,

    // scan optimization
    PushProjectIntoTableSourceScanRule.INSTANCE(),
    PushFilterIntoTableSourceScanRule.INSTANCE(),

    // unnest rule
    LogicalUnnestRule.INSTANCE(),

    // translate to flink logical rel nodes
    FlinkLogicalAggregate.CONVERTER(),
    FlinkLogicalWindowAggregate.CONVERTER(),
    FlinkLogicalOverWindow.CONVERTER(),
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
