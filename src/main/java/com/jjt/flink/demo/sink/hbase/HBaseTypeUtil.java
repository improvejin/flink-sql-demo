package com.jjt.flink.demo.sink.hbase;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.hadoop.hbase.util.Bytes;

import java.math.BigDecimal;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Date;
import java.util.HashMap;
import java.util.Map;

import static org.apache.flink.api.common.typeinfo.BasicTypeInfo.BIG_DEC_TYPE_INFO;
import static org.apache.flink.api.common.typeinfo.BasicTypeInfo.BOOLEAN_TYPE_INFO;
import static org.apache.flink.api.common.typeinfo.BasicTypeInfo.BYTE_TYPE_INFO;
import static org.apache.flink.api.common.typeinfo.BasicTypeInfo.DOUBLE_TYPE_INFO;
import static org.apache.flink.api.common.typeinfo.BasicTypeInfo.FLOAT_TYPE_INFO;
import static org.apache.flink.api.common.typeinfo.BasicTypeInfo.INT_TYPE_INFO;
import static org.apache.flink.api.common.typeinfo.BasicTypeInfo.LONG_TYPE_INFO;
import static org.apache.flink.api.common.typeinfo.BasicTypeInfo.SHORT_TYPE_INFO;
import static org.apache.flink.api.common.typeinfo.BasicTypeInfo.STRING_TYPE_INFO;
import static org.apache.flink.api.common.typeinfo.SqlTimeTypeInfo.TIMESTAMP;
import static org.apache.flink.api.common.typeinfo.SqlTimeTypeInfo.DATE;
import static org.apache.flink.api.common.typeinfo.SqlTimeTypeInfo.TIME;

public class HBaseTypeUtil {

    private static final Map<TypeInformation<?>, String> TYPE_MAPPING = new HashMap<>();

    static {
        TYPE_MAPPING.put(BOOLEAN_TYPE_INFO, "BOOLEAN");
        TYPE_MAPPING.put(BYTE_TYPE_INFO, "BYTE");
        TYPE_MAPPING.put(SHORT_TYPE_INFO, "SHORT");
        TYPE_MAPPING.put(INT_TYPE_INFO, "INT");
        TYPE_MAPPING.put(LONG_TYPE_INFO, "LONG");
        TYPE_MAPPING.put(FLOAT_TYPE_INFO, "FLOAT");
        TYPE_MAPPING.put(DOUBLE_TYPE_INFO, "DOUBLE");
        TYPE_MAPPING.put(BIG_DEC_TYPE_INFO, "BIG_DECIMAL");
        TYPE_MAPPING.put(STRING_TYPE_INFO, "STRING");
        TYPE_MAPPING.put(TIMESTAMP, "TIMESTAMP");
        TYPE_MAPPING.put(DATE, "DATE");
        TYPE_MAPPING.put(TIME, "TIME");
    }

    public static byte[] toByte(Object value, TypeInformation<?> columnType) {
        if(TYPE_MAPPING.containsKey(columnType) == false) {
            throw new RuntimeException(String.format("type %s not support in hbase sink", columnType));
        }

        switch (TYPE_MAPPING.get(columnType)) {
            case "BOOLEAN":
                if ((Boolean) value) {
                    return Bytes.toBytes(true);
                } else {
                    return Bytes.toBytes(false);
                }
            case "BYTE":
                return new byte[] { ((Byte) value).byteValue() };
            case "SHORT":
                return Bytes.toBytes(((Short) value).shortValue());
            case "INT":
                return Bytes.toBytes(((Integer) value).intValue());
            case "LONG":
                return Bytes.toBytes(((Long) value).longValue());
            case "FLOAT":
                return Bytes.toBytes(((Float) value).floatValue());
            case "DOUBLE":
                return Bytes.toBytes(((Double) value).doubleValue());
            case "BIG_DECIMAL":
                return Bytes.toBytes((BigDecimal) value);
            case "STRING":
                return Bytes.toBytes((String)value);
            case "TIMESTAMP":
                Timestamp ts = (Timestamp)value;
                return Bytes.toBytes(ts.getTime());
            case "DATE":
                Date d = (Date)value;
                return Bytes.toBytes(d.toString());
            case "TIME":
                Time t = (Time)value;
                return Bytes.toBytes(t.toString());
            default:
                 return Bytes.toBytes(value.toString());
        }
    }
}
