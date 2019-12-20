package com.jjt.flink.demo.sink;

import com.jjt.flink.demo.jobs.JobConfig;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.table.sinks.TableSink;
import org.apache.flink.types.Row;

public class ConsoleTableSink extends AbstractTableSink{

    public ConsoleTableSink(JobConfig jobConfig) {
        super(jobConfig);
    }

    public ConsoleTableSink(JobConfig jobConfig, String[] fieldNames, TypeInformation[] fieldTypes) {
        super(jobConfig, fieldNames, fieldTypes);
    }

    @Override
    public void emitDataStream(DataStream<Row> dataStream) {
        dataStream.print();
    }

    @Override
    public TableSink<Row> configure(String[] fieldNames, TypeInformation<?>[] fieldTypes) {

        return new ConsoleTableSink(this.config, fieldNames, fieldTypes);
    }
}
