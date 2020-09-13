package com.deepexi.topsports.dmp.flink.sql.oracle.connector;

import java.util.List;

public class OracleTable {
    private List<ColumnSchema> ColumnSchema;
    private  String name;
    private  String schema;

    public List<com.deepexi.topsports.dmp.flink.sql.oracle.connector.ColumnSchema> getColumnSchema() {
        return ColumnSchema;
    }

    public void setColumnSchema(List<com.deepexi.topsports.dmp.flink.sql.oracle.connector.ColumnSchema> columnSchema) {
        ColumnSchema = columnSchema;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getSchema() {
        return schema;
    }

    public void setSchema(String schema) {
        this.schema = schema;
    }

    public OracleTable(List<com.deepexi.topsports.dmp.flink.sql.oracle.connector.ColumnSchema> columnSchema, String name, String schema) {
        ColumnSchema = columnSchema;
        this.name = name;
        this.schema = schema;
    }
}