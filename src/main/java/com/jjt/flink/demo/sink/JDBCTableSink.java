package com.jjt.flink.demo.sink;

import com.jjt.flink.demo.jobs.JobConfig;
import com.jjt.flink.demo.sink.jdbc.JDBCOutputFormat;
import com.jjt.flink.demo.sink.jdbc.JDBCSinkFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.table.sinks.TableSink;
import org.apache.flink.table.util.TableConnectorUtil;
import org.apache.flink.types.Row;


public class JDBCTableSink extends AbstractTableSink {

    private String tableName;
    private final JDBCOutputFormat outputFormat;

    public JDBCTableSink(JobConfig jobConfig, String tableName) {
        super(jobConfig);
        this.tableName = tableName;
        this.outputFormat = null;
    }

    public JDBCTableSink(JobConfig config, String[] fieldNames, TypeInformation[] fieldTypes, String tableName){
        super(config, fieldNames, fieldTypes);
        this.outputFormat = new JDBCOutputFormat(config, fieldNames, fieldTypes, tableName);
    }


    @Override
    public void emitDataStream(DataStream<Row> dataStream) {
        dataStream.addSink(new JDBCSinkFunction(outputFormat))
                .name(TableConnectorUtil.generateRuntimeName(this.getClass(), fieldNames));
    }

    @Override
    public TableSink<Row> configure(String[] fieldNames, TypeInformation<?>[] fieldTypes) {
        JDBCTableSink copy = new JDBCTableSink(this.config, fieldNames, fieldTypes, this.tableName);
        return copy;
    }
}
