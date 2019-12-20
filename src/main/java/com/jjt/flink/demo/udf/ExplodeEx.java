package com.jjt.flink.demo.udf;

import org.apache.flink.table.functions.TableFunction;

@UDF(name="explode_ex", desc="explode array with empty string")
public class ExplodeEx extends TableFunction<String> {

    public void eval(String[] array) {
        collect("");
        if (array != null) {
            for(String s: array) {
                collect(s);
            }
        }
    }
}
