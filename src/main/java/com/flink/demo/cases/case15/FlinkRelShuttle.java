package com.flink.demo.cases.case15;

import org.apache.calcite.linq4j.Ord;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelShuttle;
import org.apache.calcite.rel.core.TableFunctionScan;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.logical.*;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;

/**
 * Created by P0007 on 2019/9/18.
 */
public class FlinkRelShuttle implements RelShuttle {

    protected final Deque<RelNode> stack = new ArrayDeque<>();

    /**
     * Visits a particular child of a parent.
     */
    protected RelNode visitChild(RelNode parent, int i, RelNode child) {
        stack.push(parent);
        try {
            RelNode child2 = child.accept(this);
            if (child2 != child) {
                final List<RelNode> newInputs = new ArrayList<>(parent.getInputs());
                newInputs.set(i, child2);
                return parent.copy(parent.getTraitSet(), newInputs);
            }
            return parent;
        } finally {
            stack.pop();
        }
    }

    protected RelNode visitChildren(RelNode rel) {
        for (Ord<RelNode> input : Ord.zip(rel.getInputs())) {
            rel = visitChild(rel, input.i, input.e);
        }
        return rel;
    }

    public RelNode visit(LogicalAggregate aggregate) {
        return visitChild(aggregate, 0, aggregate.getInput());
    }

    public RelNode visit(LogicalMatch match) {
        return visitChild(match, 0, match.getInput());
    }

    public RelNode visit(TableScan scan) {
        return scan;
    }

    public RelNode visit(TableFunctionScan scan) {
        return visitChildren(scan);
    }

    public RelNode visit(LogicalValues values) {
        return values;
    }

    public RelNode visit(LogicalFilter filter) {
        return visitChild(filter, 0, filter.getInput());
    }

    public RelNode visit(LogicalProject project) {
        return visitChild(project, 0, project.getInput());
    }

    public RelNode visit(LogicalJoin join) {
        return visitChildren(join);
    }

    public RelNode visit(LogicalCorrelate correlate) {
        return visitChildren(correlate);
    }

    public RelNode visit(LogicalUnion union) {
        return visitChildren(union);
    }

    public RelNode visit(LogicalIntersect intersect) {
        return visitChildren(intersect);
    }

    public RelNode visit(LogicalMinus minus) {
        return visitChildren(minus);
    }

    public RelNode visit(LogicalSort sort) {
        return visitChildren(sort);
    }

    public RelNode visit(LogicalExchange exchange) {
        return visitChildren(exchange);
    }

    public RelNode visit(RelNode other) {
        return visitChildren(other);
    }

}
