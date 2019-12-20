package com.jjt.flink.demo.sink;

import com.jjt.flink.demo.jobs.JobConfig;
import com.jjt.flink.demo.sink.hbase.HBaseSinkFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.table.sinks.TableSink;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;

public class HBaseTableSink extends AbstractTableSink {

    private static final Logger LOG = LoggerFactory.getLogger(HBaseTableSink.class);

    private String tableName;

    public HBaseTableSink(JobConfig jobConfig, String tableName) {
        super(jobConfig);
        this.tableName = tableName;
    }

    public HBaseTableSink(JobConfig jobConfig, String tableName,  String[] fieldNames, TypeInformation[] fieldTypes) {
        super(jobConfig, fieldNames, fieldTypes);
        this.tableName = tableName;
    }

    @Override
    public void emitDataStream(DataStream<Row> dataStream) {
        dataStream.addSink(new HBaseSinkFunction(tableName, fieldNames, fieldTypes)).name("hbase sink");
    }

    @Override
    public TableSink<Row> configure(String[] fieldNames, TypeInformation<?>[] fieldTypes) {
        List<String> fields = Arrays.asList(fieldNames);
        if(fields.contains("hkey") == false) {
            throw new RuntimeException("hbase table sink must configure hkey");

        } else {
            if (fields.contains("ts") == false) {
                LOG.warn("there is no timestamp in output columns, use process time as timestamp");
            }
            return  new HBaseTableSink(config, tableName, fieldNames, fieldTypes);
        }
    }
}
