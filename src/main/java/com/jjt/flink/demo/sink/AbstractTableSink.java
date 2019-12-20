package com.jjt.flink.demo.sink;

import com.jjt.flink.demo.jobs.JobConfig;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.table.sinks.AppendStreamTableSink;
import org.apache.flink.types.Row;

public abstract class AbstractTableSink implements AppendStreamTableSink<Row> {

    protected JobConfig config;
    protected String[] fieldNames;
    protected TypeInformation[] fieldTypes;

    protected AbstractTableSink(JobConfig jobConfig) {
        this.config = jobConfig;
    }

    protected  AbstractTableSink(JobConfig jobConfig, String[] fieldNames, TypeInformation[] fieldTypes) {
        this.config = jobConfig;
        this.fieldNames = fieldNames;
        this.fieldTypes = fieldTypes;
    }

    @Override
    public TypeInformation<Row> getOutputType() {
        return new RowTypeInfo(fieldTypes, fieldNames);
    }

    @Override
    public String[] getFieldNames() {
        return fieldNames;
    }

    @Override
    public TypeInformation<?>[] getFieldTypes() {
        return fieldTypes;
    }
}
