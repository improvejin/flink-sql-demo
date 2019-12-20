package com.jjt.flink.demo.sink.hbase;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.types.Row;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;

import java.util.ArrayList;
import java.util.List;

public class HBaseSinkFunction extends RichSinkFunction<Row> implements CheckpointedFunction {

    private final int BUFFER_SIZE = 10;

    private String table;
    private Connection connection;
    private BufferedMutator mutator;
    private List<Put> putLists;
    private int keyIndex;
    private int tsIndex;
    private String[] fieldNames;
    private TypeInformation[] fieldTypes;

    public HBaseSinkFunction(String table, String[] fieldNames, TypeInformation[] fieldTypes) {
        this.table = table;
        this.putLists = new ArrayList();
        this.fieldNames = fieldNames;
        this.fieldTypes = fieldTypes;
        this.keyIndex = getIndex(fieldNames, "hkey");
        assert this.keyIndex != -1;
        this.tsIndex = getIndex(fieldNames, "ts");
    }

    private static int getIndex(String[] fields, String f) {
        for(int i = 0; i < fields.length; ++i) {
            if(fields[i].equals(f)) {
                return i;
            }
        }
        return -1;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        org.apache.hadoop.conf.Configuration configuration = HBaseConfiguration.create();
        this.connection = ConnectionFactory.createConnection(configuration);
        BufferedMutatorParams params = new BufferedMutatorParams(TableName.valueOf(this.table));
        this.mutator = connection.getBufferedMutator(params);
    }

    @Override
    public void close() throws Exception {
        mutator.flush();
        connection.close();
    }

    private Tuple2<String, String> getColumnMeta(int fieldIndex) {
        String[] s = fieldNames[fieldIndex].split("__");
        if(s.length == 2) {
            return new Tuple2(s[0], s[1]);
        } else {
            return new Tuple2("default_cf", s[0]);
        }
    }

    @Override
    public void invoke(Row value, Context context) throws Exception {
        long ts;
        if(this.tsIndex != -1) {
            ts = (long)value.getField(this.tsIndex);
        } else {
            ts = context.currentProcessingTime();
        }
        byte[] rowKey = HBaseTypeUtil.toByte(value.getField(this.keyIndex), this.fieldTypes[this.keyIndex]);
        Put p = new Put(rowKey);
        for(int i = 0; i < value.getArity(); i++) {
            if(i == this.keyIndex || i == this.tsIndex) {
                continue;
            }
            Object v = value.getField(i);
            if (v != null) {
                Tuple2<String, String> columnMeta = getColumnMeta(i);
                p.addColumn(columnMeta.f0.getBytes(), columnMeta.f1.getBytes(), ts, HBaseTypeUtil.toByte(v, this.fieldTypes[i]));
            }
        }

        putLists.add(p);
        if(putLists.size() > BUFFER_SIZE) {
            mutator.mutate(putLists);
            mutator.flush();
            putLists.clear();
        }
    }

    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {
        mutator.mutate(putLists);
        mutator.flush();
    }

    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {

    }

}