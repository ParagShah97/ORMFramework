package com.thinking.machines.dmframework.utilities;

import com.thinking.machines.dmframework.exceptions.*;
import java.util.*;
import com.thinking.machines.dmframework.pojo.*;

public class Types {
    private static HashMap<Integer, DataType> types;
    private static HashSet<Class> numericDataTypes;
    static {
        numericDataTypes = new HashSet<Class>();
        types = new HashMap<Integer, DataType>();
        DataType dataType;
        dataType = new DataType();
        dataType.setType(Boolean.class);
        dataType.setPrimitiveType(boolean.class);
        dataType.setSetterGetterMethodName("Boolean");
        dataType.setDefaultValue("false");
        types.put(java.sql.Types.BIT, dataType);
        dataType = new DataType();
        dataType.setType(Integer.class);
        dataType.setPrimitiveType(int.class);
        dataType.setSetterGetterMethodName("Int");
        dataType.setDefaultValue("0");
        types.put(java.sql.Types.INTEGER, dataType);
        types.put(java.sql.Types.TINYINT, dataType);
        types.put(java.sql.Types.SMALLINT, dataType);
        numericDataTypes.add(Integer.class);
        dataType = new DataType();
        dataType.setType(Long.class);
        dataType.setPrimitiveType(long.class);
        dataType.setSetterGetterMethodName("Long");
        dataType.setDefaultValue("0L");
        types.put(java.sql.Types.BIGINT, dataType);
        numericDataTypes.add(Long.class);
        dataType = new DataType();
        dataType.setType(java.math.BigDecimal.class);
        dataType.setPrimitiveType(java.math.BigDecimal.class);
        dataType.setSetterGetterMethodName("BigDecimal");
        dataType.setDefaultValue("\"0.00\"");
        types.put(java.sql.Types.DECIMAL, dataType);
        types.put(java.sql.Types.NUMERIC, dataType);
        numericDataTypes.add(java.math.BigDecimal.class);
        // we want that our implementation should not support double float at db level
        // types.put(java.sql.Types.FLOAT,dataType);
        // types.put(java.sql.Types.DOUBLE,dataType);
        dataType = new DataType();
        dataType.setType(String.class);
        dataType.setPrimitiveType(String.class);
        dataType.setSetterGetterMethodName("String");
        dataType.setDefaultValue("\"\"");
        types.put(java.sql.Types.CHAR, dataType);
        types.put(java.sql.Types.VARCHAR, dataType);
        types.put(java.sql.Types.LONGVARCHAR, dataType);
        types.put(java.sql.Types.NCHAR, dataType);
        types.put(java.sql.Types.NVARCHAR, dataType);
        dataType = new DataType();
        dataType.setType(Boolean.class);
        dataType.setPrimitiveType(boolean.class);
        dataType.setSetterGetterMethodName("Boolean");
        dataType.setDefaultValue("false");
        types.put(java.sql.Types.BOOLEAN, dataType);
        dataType = new DataType();
        dataType.setType(java.sql.Date.class);
        dataType.setPrimitiveType(java.sql.Date.class);
        dataType.setSetterGetterMethodName("Date");
        dataType.setDefaultValue("new java.sql.Date(new java.util.Date().getTime())");
        types.put(java.sql.Types.DATE, dataType);
        dataType = new DataType();
        dataType.setType(java.sql.Time.class);
        dataType.setPrimitiveType(java.sql.Time.class);
        dataType.setSetterGetterMethodName("Time");
        dataType.setDefaultValue("new java.sql.Time(new java.util.Date().getTime())");
        types.put(java.sql.Types.TIME, dataType);
        dataType = new DataType();
        dataType.setType(java.sql.Timestamp.class);
        dataType.setPrimitiveType(java.sql.Timestamp.class);
        dataType.setSetterGetterMethodName("Timestamp");
        dataType.setDefaultValue("new java.sql.Timestamp(new java.util.Date().getTime())");
        types.put(java.sql.Types.TIMESTAMP, dataType);
        // for Point type
        /*
         * dataType=new DataType();
         * dataType.setType(String.class);
         * dataType.setPrimitiveType(String.class);
         * dataType.setSetterGetterMethodName("String");
         * dataType.setDefaultValue("\"0 0\"");
         * types.put(java.sql.Types.OTHER,dataType);
         */
        // types.put(java.sql.Types.REAL,);
        // types.put(java.sql.Types.BINARY,);
        // types.put(java.sql.Types.VARBINARY,);
        // types.put(java.sql.Types.LONGVARBINARY,);
        // types.put(java.sql.Types.NULL,);
        // types.put(java.sql.Types.OTHER,);
        // types.put(java.sql.Types.JAVA_OBJECT,);
        // types.put(java.sql.Types.DISTINCT,);
        // types.put(java.sql.Types.STRUCT,);
        // types.put(java.sql.Types.ARRAY,);
        // types.put(java.sql.Types.BLOB,);
        // types.put(java.sql.Types.CLOB,);
        // types.put(java.sql.Types.REF,);
        // types.put(java.sql.Types.DATALINK,);
        // types.put(java.sql.Types.ROWID,);
        // types.put(java.sql.Types.LONGNVARCHAR,);
        // types.put(java.sql.Types.NCLOB,);
        // types.put(java.sql.Types.SQLXML,);
        // types.put(java.sql.Types.REF_CURSOR,);
        // types.put(java.sql.Types.TIME_WITH_TIMEZONE,);
        // types.put(java.sql.Types.TIMESTAMP_WITH_TIMEZONE,);
    }

    public static boolean isNumeric(Class c) {
        return numericDataTypes.contains(c);
    }

    public static DataType getDataType(int type) throws DMFrameworkException {
        DataType dataType = types.get(type);
        if (dataType == null)
            throw new DMFrameworkException("Invalid type : " + type);
        return dataType;
    }

    public static Class getJavaType(int type) throws DMFrameworkException {
        DataType dataType = types.get(type);
        if (dataType == null)
            throw new DMFrameworkException("Invalid type : " + type);
        return dataType.getType();
    }
}