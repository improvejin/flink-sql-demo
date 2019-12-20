package com.jjt.flink.demo.sink;

import com.jjt.flink.demo.jobs.JobConfig;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer08;
import org.apache.flink.table.sinks.TableSink;
import org.apache.flink.types.Row;

import java.util.Properties;

public class KafkaTableSink  extends AbstractTableSink{

    private String topic;


    public KafkaTableSink(JobConfig jobConfig, String topic) {
        super(jobConfig);
        this.topic = topic;
    }

    public KafkaTableSink(JobConfig config, String[] fieldNames, TypeInformation[] fieldTypes, String topic) {
        super(config, fieldNames, fieldTypes);
        this.topic = topic;
    }

    @Override
    public void emitDataStream(DataStream<Row> dataStream) {
        Properties properties = this.config.getProducerProp();
        FlinkKafkaProducer08<Row> producer = new FlinkKafkaProducer08<>(topic, new KafkaSerialization(), properties);
        dataStream.addSink(producer);
    }

    @Override
    public TableSink<Row> configure(String[] fieldNames, TypeInformation<?>[] fieldTypes) {
        return new KafkaTableSink(this.config, fieldNames, fieldTypes, this.topic);
    }


    public static class KafkaSerialization implements SerializationSchema<Row> {
        @Override
        public byte[] serialize(Row row) {
            return row.toString().getBytes();
        }
    }
}
