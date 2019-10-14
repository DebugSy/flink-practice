package com.flink.demo.cases.case17;

import com.codahale.metrics.Meter;
import com.codahale.metrics.RatioGauge;

/**
 * Created by P0007 on 2019/10/11.
 */
public class CacheHitRatio extends RatioGauge {

    private final Meter hits;
    private final Meter calls;

    public CacheHitRatio(Meter hits, Meter calls) {
        this.hits = hits;
        this.calls = calls;
    }

    @Override
    public Ratio getRatio() {
        return Ratio.of(hits.getOneMinuteRate(),
                calls.getOneMinuteRate());
    }
}
