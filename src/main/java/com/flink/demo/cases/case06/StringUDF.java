package com.flink.demo.cases.case06;

import org.apache.flink.table.functions.ScalarFunction;

/**
 * Created by P0007 on 2020/4/3.
 */
public class StringUDF extends ScalarFunction {

    /**
     * 按separator截取string，并返回第num个元素
     * @param string
     * @return
     */
    public String eval(String string, String separator, int num) {
        int count = 0;
        int nextIndex = 0;
        int fromIndex = 0;
        int subStrIndex = 0;
        while (count != num) {
            subStrIndex = fromIndex;
            nextIndex = string.indexOf(separator, fromIndex);
            fromIndex = nextIndex + 1;
            count++;
        }
        String res = string.substring(subStrIndex, nextIndex);
        return res;
    }


    public static void main(String[] args) {
        String string = "1,2a,3dadada,4lkdsasd,5ffffffffffffffffffffffffff,6";
        String eval = new StringUDF().eval(string, ",", 3);
        System.out.println(string);
        System.out.println(eval);
    }

}
