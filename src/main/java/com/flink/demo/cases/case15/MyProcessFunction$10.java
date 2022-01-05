package com.flink.demo.cases.case15;

/**
 * Created by P0007 on 2019/9/24.
 */
public class MyProcessFunction$10 extends org.apache.flink.streaming.api.functions.ProcessFunction {


    final org.apache.flink.types.Row out =
            new org.apache.flink.types.Row(4);



    public MyProcessFunction$10() throws Exception {


    }




    @Override
    public void open(org.apache.flink.configuration.Configuration parameters) throws Exception {


    }

    @Override
    public void processElement(Object _in1, org.apache.flink.streaming.api.functions.ProcessFunction.Context ctx, org.apache.flink.util.Collector c) throws Exception {
        org.apache.flink.types.Row in1 = (org.apache.flink.types.Row) _in1;

        boolean isNull$5 = (java.lang.String) in1.getField(2) == null;
        java.lang.String result$4;
        if (isNull$5) {
            result$4 = "";
        }
        else {
            result$4 = (java.lang.String) (java.lang.String) in1.getField(2);
        }


        boolean isNull$1 = (java.lang.Integer) in1.getField(0) == null;
        int result$0;
        if (isNull$1) {
            result$0 = -1;
        }
        else {
            result$0 = (java.lang.Integer) in1.getField(0);
        }


        boolean isNull$3 = (java.lang.String) in1.getField(1) == null;
        java.lang.String result$2;
        if (isNull$3) {
            result$2 = "";
        }
        else {
            result$2 = (java.lang.String) (java.lang.String) in1.getField(1);
        }


        boolean isNull$7 = (java.sql.Timestamp) in1.getField(3) == null;
        long result$6;
        if (isNull$7) {
            result$6 = -1L;
        }
        else {
            result$6 = (long) org.apache.calcite.runtime.SqlFunctions.toLong((java.sql.Timestamp) in1.getField(3));
        }








        java.lang.String result$9 = "user A";

        boolean isNull$11 = isNull$3 || false;
        boolean result$10;
        if (isNull$11) {
            result$10 = false;
        }
        else {
            result$10 = result$2.compareTo(result$9) == 0;
        }


        boolean result$15 = false;
        boolean isNull$16 = false;
        if (!isNull$11 && !result$10) {
            // left expr is false, result is always false
            // skip right expr
        } else {



            int result$12 = 60;

            boolean isNull$14 = isNull$1 || false;
            boolean result$13;
            if (isNull$14) {
                result$13 = false;
            }
            else {
                result$13 = result$0 > result$12;
            }


            if (isNull$11) {
                // left is null (unknown)
                if (isNull$14 || result$13) {
                    isNull$16 = true;
                }
            } else {
                // left is true
                if (isNull$14) {
                    isNull$16 = true;
                } else if (result$13) {
                    result$15 = true;
                }
            }
        }

        if (result$15) {


            if (isNull$1) {
                out.setField(0, null);
            }
            else {
                out.setField(0, result$0);
            }



            if (isNull$3) {
                out.setField(1, null);
            }
            else {
                out.setField(1, result$2);
            }



            if (isNull$5) {
                out.setField(2, null);
            }
            else {
                out.setField(2, result$4);
            }




            java.sql.Timestamp result$8;
            if (isNull$7) {
                result$8 = null;
            }
            else {
                result$8 = org.apache.calcite.runtime.SqlFunctions.internalToTimestamp(result$6);
            }

            if (isNull$7) {
                out.setField(3, null);
            }
            else {
                out.setField(3, result$8);
            }

            c.collect(out);
        }
    }

    @Override
    public void close() throws Exception {


    }
}
