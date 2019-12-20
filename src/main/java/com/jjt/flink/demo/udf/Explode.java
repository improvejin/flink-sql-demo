package com.jjt.flink.demo.udf;

import org.apache.flink.table.functions.TableFunction;

@UDF(name="explode", desc="explode array")
public class Explode extends TableFunction<String> {

    public void eval(String[] array) {
        if(array != null) {
            for(String s: array) {
                collect(s);
            }
        }
    }
}
