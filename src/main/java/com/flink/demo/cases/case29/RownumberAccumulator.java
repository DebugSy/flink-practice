package com.flink.demo.cases.case29;

import lombok.Getter;
import lombok.Setter;
import org.apache.flink.types.Row;

import java.util.SortedMap;
import java.util.TreeMap;

@Getter
@Setter
public class RownumberAccumulator {

    private SortedMap<Row, Long> treeMap = new TreeMap<>();

    private Long rank;

}
