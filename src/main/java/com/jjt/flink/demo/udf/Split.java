package com.jjt.flink.demo.udf;

import org.apache.flink.table.functions.ScalarFunction;

@UDF(name="split", desc="split string")
public class Split extends ScalarFunction {

    public String[] eval(String s, String regex) {
        return s.split(regex);
    }
}
