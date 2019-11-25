package com.flink.demo.cases.case20;

import java.sql.Timestamp;

/**
 * Created by P0007 on 2019/11/4.
 */
public class MyTest {

    public static void main(String[] args) {
        long time = 1572869769342L;
        System.out.println(new Timestamp(time));

        long minuteTime = time / 60000 * 60000;
        System.out.println(new Timestamp(minuteTime));
    }

}
