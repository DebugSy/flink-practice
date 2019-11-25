package com.flink.demo.cases.case20;

import java.io.Serializable;

/**
 * Created by P0007 on 2019/11/4.
 */
public class NokiaAcc implements Serializable {

    public NokiaAcc() {
    }

    public NokiaAcc(long total, long empty) {
        this.total = total;
        this.empty = empty;
    }

    public long total;

    public long empty;

}