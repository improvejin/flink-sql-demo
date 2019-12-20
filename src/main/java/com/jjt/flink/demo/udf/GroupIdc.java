package com.jjt.flink.demo.udf;

import org.apache.flink.table.functions.TableFunction;

@UDF(name="group_idc", desc="statistic flow of idc")
public class GroupIdc extends TableFunction<String> {

    public void eval(String host) {
        collect("");

        String idc = host.substring(0, 3);
        if(idc != null) {
            collect(idc);
        }
    }
}
