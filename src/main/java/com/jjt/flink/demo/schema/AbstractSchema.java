package com.jjt.flink.demo.schema;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.PojoField;
import org.apache.flink.api.java.typeutils.PojoTypeInfo;
import org.apache.flink.table.api.Types;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.IOException;
import java.lang.reflect.Field;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

abstract public class AbstractSchema<T extends AbstractSchema> implements DeserializationSchema<T> {

    private static final Logger logger = LoggerFactory.getLogger(AbstractSchema.class);

    private static Map<Class, TypeInformation> types = new HashMap<>();

    static {
        types.put(String.class, Types.STRING());
        types.put(Boolean.class, Types.BOOLEAN());
        types.put(Byte.class, Types.BYTE());
        types.put(Short.class, Types.SHORT());
        types.put(Integer.class, Types.INT());
        types.put(Long.class, Types.LONG());
        types.put(Float.class, Types.FLOAT());
        types.put(Double.class, Types.DOUBLE());
        types.put(BigDecimal.class, Types.DECIMAL());
        types.put(java.sql.Date.class, Types.SQL_DATE());
        types.put(java.sql.Time.class, Types.SQL_TIME());
        types.put(java.sql.Timestamp.class, Types.SQL_TIMESTAMP());
        types.put(Map.class, Types.MAP(Types.STRING(), Types.STRING()));
        types.put(String[].class, Types.OBJECT_ARRAY(Types.STRING()));
    }

    /**
     * get the TypeInformation of the schema
     * @return
     */
    @Override
    public TypeInformation<T> getProducedType() {
        List<PojoField> fieldsType = new ArrayList<>();
        Field[] fields = this.getClass().getFields();
        for(Field field: fields) {
            fieldsType.add(new PojoField(field, types.get(field.getType())));
        }
        return new PojoTypeInfo(this.getClass(), fieldsType);
    }

//    /***
//     * get table schema for sql query
//     * @return
//     */
//    public TableSchema getTableSchema() {
//        List<String> fieldNames = new ArrayList<>();
//        List<TypeInformation> fieldTypes = new ArrayList<>();
//        Field[] fields = this.getClass().getFields();
//        for(Field f: fields) {
//            fieldNames.add(f.getName());
//            fieldTypes.add(types.get(f.getType()));
//        }
//        return TableSchema.fromTypeInfo(Types.ROW(fieldNames.toArray(new String[0]), fieldTypes.toArray(new TypeInformation[0])));
//    }

    /**
     * get timestamp attribute which is field in table schema
     * @return
     */
    public String getTimeStampAttribute() {
        return "ts";
    }

    @Override
    public boolean isEndOfStream(T var1) {
        return false;
    }



    @Override
    public T deserialize(byte[] bytes) throws IOException {
        String msg = new String(bytes);
        String[] data = msg.split("\\t");
        Field[] fields = this.getClass().getFields();
        Object instance;
        try {
            instance = this.getClass().newInstance();
            for(int i = 0; i < fields.length; ++i) {
                Field field = fields[i];
                field.set(instance, parseFieldValue(field, data[i]));
            }
        } catch (Exception e) {
            instance = null; // null instance will be ignored by flink
            logger.error(msg);
            logger.error("parse error", e);
        }
        return (T)instance;
    }

    private static Object parseFieldValue(Field field, String val) {
        if ("\\N".equals(val)) {
            return null;
        }

        Class type = field.getType();

        if (type == String.class) {
            return val;
        } else if (type == Boolean.class) {
            return Boolean.valueOf(val);
        } else if (type == Byte.class) {
            return Byte.valueOf(val);
        } else if (type == Short.class) {
            return Short.valueOf(val);
        } else if (type == Integer.class) {
            return Integer.valueOf(val);
        } else if (type == Long.class) {
            return Long.valueOf(val);
        } else if (type == Float.class) {
            return Float.valueOf(val);
        } else if (type == Double.class) {
            return Double.valueOf(val);
        } else if (type == java.math.BigDecimal.class) {
            throw new UnsupportedOperationException("type BigDecimal is not support");
        }
        else if (type == java.sql.Time.class) {
            return java.sql.Time.valueOf(val);
        } else if (type == java.sql.Date.class) {
            return java.sql.Date.valueOf(val);
        } else if(type == java.sql.Timestamp.class) {
            //type of timestamp form message is LONG
            return new java.sql.Timestamp(Long.valueOf(val));
        }
        //以下复杂类型不再校验成员类型,限定成员类型只能是String
        else if (type == String[].class) {
            return val.split(",");
        } else if (type == Map.class) {
            Map<String, String> map = new HashMap<>();
            String[] arr = val.split(",");
            for (String s : arr) {
                String[] kv = s.split(":");
                if (kv.length == 2) {
                    String key = kv[0].trim();
                    String value = kv[1].trim();
                    if ("\\N".equals(value)) {
                        map.put(key, null);
                    } else {
                        map.put(key, value);
                    }
                }
            }
            return map;
        }

        return null;
    }
}
