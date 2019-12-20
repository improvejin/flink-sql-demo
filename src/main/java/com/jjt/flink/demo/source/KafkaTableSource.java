package com.jjt.flink.demo.source;

import com.jjt.flink.demo.schema.AbstractSchema;
import com.jjt.flink.demo.jobs.JobConfig;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer08;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.sources.DefinedRowtimeAttributes;
import org.apache.flink.table.sources.RowtimeAttributeDescriptor;
import org.apache.flink.table.sources.StreamTableSource;
import org.apache.flink.table.sources.tsextractors.ExistingField;
import org.apache.flink.table.sources.wmstrategies.BoundedOutOfOrderTimestamps;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class KafkaTableSource<T extends AbstractSchema> implements StreamTableSource<T>, DefinedRowtimeAttributes {

    private String topic;
    private Properties props;
    private int latency;
    private T instance;

    public KafkaTableSource(String source, JobConfig jobConfig, Class<T> s) {
        this.topic = jobConfig.getTopicFromFile(source);
        this.latency = jobConfig.getLatency();
        this.props = jobConfig.getConsumerProp(this.topic);
        try {
            this.instance = s.newInstance();
        } catch (Exception e) {
            throw new RuntimeException("cannot new instance of the schema", e);
        }
    }

    @Override
    public DataStream<T> getDataStream(StreamExecutionEnvironment env) {
        FlinkKafkaConsumer08<T> kafkaSource = new FlinkKafkaConsumer08<T>(topic, instance, props);
        kafkaSource.setStartFromLatest();
        DataStream<T> input = env.addSource(kafkaSource);
        return input;
    }

    @Override
    public TypeInformation<T> getReturnType() {
        return instance.getProducedType();
    }

    @Override
    public TableSchema getTableSchema() {
        return TableSchema.fromTypeInfo(getReturnType());
    }

    @Override
    public String explainSource() {
        return "KafkaTableSource";
    }

    @Override
    public List<RowtimeAttributeDescriptor> getRowtimeAttributeDescriptors() {
        String tsAttribute = instance.getTimeStampAttribute();
        RowtimeAttributeDescriptor ds = new RowtimeAttributeDescriptor(tsAttribute, new ExistingField(tsAttribute),
                new BoundedOutOfOrderTimestamps(TimeUnit.SECONDS.toMillis(this.latency)));
        List<RowtimeAttributeDescriptor> array = new ArrayList();
        array.add(ds);
        return array;
    }
}
