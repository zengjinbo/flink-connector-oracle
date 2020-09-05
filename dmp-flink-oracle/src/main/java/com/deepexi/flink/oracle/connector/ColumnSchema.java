package com.deepexi.flink.oracle.connector;


import oracle.jdbc.OracleType;

public class ColumnSchema {
    private final String name;
    private final OracleType type;
    private  boolean key;
    private  boolean nullable;
    private  Object defaultValue;
    private  int desiredBlockSize;
    private  int typeSize;
    private  int dataScale;

    public int getDataScale() {
        return dataScale;
    }

    public void setDataScale(int dataScale) {
        this.dataScale = dataScale;
    }

    public ColumnSchema(String name, OracleType type, int typeSize, int dataScale) {
        this.name = name;
        this.type = type;
        this.typeSize = typeSize;
        this.dataScale = dataScale;
    }

    public ColumnSchema(String name) {
        this.name = name;
        this.type=null;
    }

    private  String comment;

    public ColumnSchema(String name, OracleType type, boolean key, boolean nullable, Object defaultValue, int desiredBlockSize, int typeSize, String comment) {
        this.name = name;
        this.type = type;
        this.key = key;
        this.nullable = nullable;
        this.defaultValue = defaultValue;
        this.desiredBlockSize = desiredBlockSize;
        this.typeSize = typeSize;
        this.comment = comment;
    }

    public ColumnSchema(String name, OracleType type) {
        this.name = name;
        this.type = type;
    }

    public String getName() {
        return name;
    }

    public OracleType getType() {
        return type;
    }

    public boolean isKey() {
        return key;
    }

    public boolean isNullable() {
        return nullable;
    }

    public Object getDefaultValue() {
        return defaultValue;
    }

    public int getDesiredBlockSize() {
        return desiredBlockSize;
    }

    public int getTypeSize() {
        return typeSize;
    }

    public String getComment() {
        return comment;
    }

    public void setKey(boolean key) {
        this.key = key;
    }

    public void setNullable(boolean nullable) {
        this.nullable = nullable;
    }

    public void setDefaultValue(Object defaultValue) {
        this.defaultValue = defaultValue;
    }

    public void setDesiredBlockSize(int desiredBlockSize) {
        this.desiredBlockSize = desiredBlockSize;
    }

    public void setTypeSize(int typeSize) {
        this.typeSize = typeSize;
    }

    public void setComment(String comment) {
        this.comment = comment;
    }
}