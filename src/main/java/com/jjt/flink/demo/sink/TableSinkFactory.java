package com.jjt.flink.demo.sink;

import com.jjt.flink.demo.jobs.JobConfig;
import org.apache.flink.table.sinks.AppendStreamTableSink;
import org.apache.flink.types.Row;

public class TableSinkFactory {

    public static AppendStreamTableSink<Row> createTableSink(JobConfig jobConfig, String output) {
        String[] destination = output.split("\\.");
        switch (destination[0]) {
            case "kafka":
                return new KafkaTableSink(jobConfig, destination[1]);
            case "mysql":
                return new JDBCTableSink(jobConfig, destination[1]);
            case "hbase":
                return new HBaseTableSink(jobConfig, destination[1]);
            default:
                    return new ConsoleTableSink(jobConfig);
        }
    }
}
