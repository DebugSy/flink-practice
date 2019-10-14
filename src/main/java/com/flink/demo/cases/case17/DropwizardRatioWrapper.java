package com.flink.demo.cases.case17;

import org.apache.flink.metrics.Gauge;

/**
 * Created by P0007 on 2019/10/11.
 */
public class DropwizardRatioWrapper  implements Gauge<Double> {

    private CacheHitRatio cacheHitRatio;

    public DropwizardRatioWrapper(CacheHitRatio cacheHitRatio) {
        this.cacheHitRatio = cacheHitRatio;
    }

    @Override
    public Double getValue() {
        return cacheHitRatio.getValue();
    }

}
