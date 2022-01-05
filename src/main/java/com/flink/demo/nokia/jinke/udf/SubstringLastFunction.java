package com.flink.demo.nokia.jinke.udf;

import org.apache.flink.table.functions.ScalarFunction;

/**
 * Created by P0007 on 2020/3/5.
 */
public class SubstringLastFunction extends ScalarFunction {

    public String eval(String str, String findStr) {
        int lastIndexOf = str.lastIndexOf(findStr);
        String substring = str.substring(0, lastIndexOf);
        return substring;
    }

    public static void main(String[] args) {
        SubstringLastFunction substringLastFunction = new SubstringLastFunction();
        String eval = substringLastFunction.eval("FACEID-HMJ-200225561800159840", "-");
        System.out.println(eval);
    }

}
