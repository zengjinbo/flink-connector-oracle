/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.deepexi.flink.oracle;


import com.deepexi.flink.oracle.config.OracleProperties;
import com.deepexi.flink.oracle.connector.OracleTableInfo;
import com.deepexi.flink.oracle.utils.OracleTableUtils;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.descriptors.DescriptorProperties;
import org.apache.flink.table.descriptors.SchemaValidator;
import org.apache.flink.table.factories.TableSinkFactory;
import org.apache.flink.table.factories.TableSourceFactory;
import org.apache.flink.types.Row;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.flink.table.descriptors.ConnectorDescriptorValidator.CONNECTOR_TYPE;
import static org.apache.flink.table.descriptors.DescriptorProperties.*;
import static org.apache.flink.table.descriptors.Rowtime.*;
import static org.apache.flink.table.descriptors.Schema.*;
import static org.apache.flink.util.Preconditions.checkNotNull;

public class OracleTableFactory implements TableSourceFactory<Row>, TableSinkFactory<Tuple2<Boolean, Row>>{

    public static final String ORACLE_TABLE = "oracle.table";
    public static final String ORACLE_URL = "oracle.url";
    public static final String ORACLE_DRIVER = "oracle.driver";
    public static final String ORACLE_USER_NAME = "oracle.username";
    public static final String ORACLE_PASSWORD = "oracle.password";
    public static final String ORACLE_PRIMARY_KEY_COLS = "oracle.primary-key-columns";
    public static final String ORACLE_SCHEMA = "oracle.schema";
    public static final String ORACLE = "oracle";

    @Override
    public Map<String, String> requiredContext() {
        Map<String, String> context = new HashMap<>();
        context.put(CONNECTOR_TYPE, ORACLE);
        return context;
    }

    @Override
    public List<String> supportedProperties() {
        List<String> properties = new ArrayList<>();
        properties.add(ORACLE_TABLE);
        properties.add(ORACLE_URL);
        properties.add(ORACLE_DRIVER);
        properties.add(ORACLE_USER_NAME);
        properties.add(ORACLE_PASSWORD);
        properties.add(ORACLE_PRIMARY_KEY_COLS);
        properties.add(ORACLE_SCHEMA);
        // schema
        properties.add(SCHEMA + ".#." + SCHEMA_DATA_TYPE);
        properties.add(SCHEMA + ".#." + SCHEMA_TYPE);
        properties.add(SCHEMA + ".#." + SCHEMA_NAME);
        properties.add(SCHEMA + ".#." + SCHEMA_FROM);
        // computed column
        properties.add(SCHEMA + ".#." + EXPR);

        // time attributes
        properties.add(SCHEMA + ".#." + SCHEMA_PROCTIME);
        properties.add(SCHEMA + ".#." + ROWTIME_TIMESTAMPS_TYPE);
        properties.add(SCHEMA + ".#." + ROWTIME_TIMESTAMPS_FROM);
        properties.add(SCHEMA + ".#." + ROWTIME_TIMESTAMPS_CLASS);
        properties.add(SCHEMA + ".#." + ROWTIME_TIMESTAMPS_SERIALIZED);
        properties.add(SCHEMA + ".#." + ROWTIME_WATERMARKS_TYPE);
        properties.add(SCHEMA + ".#." + ROWTIME_WATERMARKS_CLASS);
        properties.add(SCHEMA + ".#." + ROWTIME_WATERMARKS_SERIALIZED);
        properties.add(SCHEMA + ".#." + ROWTIME_WATERMARKS_DELAY);

        // watermark
        properties.add(SCHEMA + "." + WATERMARK + ".#." + WATERMARK_ROWTIME);
        properties.add(SCHEMA + "." + WATERMARK + ".#." + WATERMARK_STRATEGY_EXPR);
        properties.add(SCHEMA + "." + WATERMARK + ".#." + WATERMARK_STRATEGY_DATA_TYPE);
        return properties;
    }

    private DescriptorProperties getValidatedProps(Map<String, String> properties) {
        checkNotNull(properties.get(ORACLE_URL), "Missing required property " + ORACLE_URL);
        checkNotNull(properties.get(ORACLE_TABLE), "Missing required property " + ORACLE_TABLE);
        checkNotNull(properties.get(ORACLE_USER_NAME), "Missing required property " + ORACLE_USER_NAME);
        checkNotNull(properties.get(ORACLE_PASSWORD), "Missing required property " + ORACLE_PASSWORD);
        final DescriptorProperties descriptorProperties = new DescriptorProperties(true);
        descriptorProperties.putProperties(properties);
        new SchemaValidator(true, false, false).validate(descriptorProperties);
        return descriptorProperties;
    }

    @Override
    public OracleTableSource createTableSource(Map<String, String> properties) {
        DescriptorProperties descriptorProperties = getValidatedProps(properties);
        String tableName = descriptorProperties.getString(ORACLE_TABLE);
        TableSchema schema = descriptorProperties.getTableSchema(SCHEMA);
        return createTableSource(tableName, schema, properties);
    }

    @Override
    public OracleTableSource createTableSource(ObjectPath tablePath, CatalogTable table) {
        String tableName = tablePath.getObjectName();
        return createTableSource(tableName, table.getSchema(), table.getProperties());
    }
/*    @Override
    public OracleTableSource createTableSource(TableSourceFactory.Context context) {
        return  createTableSource()
    }*/

    private OracleTableSource createTableSource(String tableName, TableSchema schema, Map<String, String> props) {
        String masterAddresses = props.get(ORACLE_URL);
      //  TableSchema physicalSchema = OracleTableUtils.getSchemaWithSqlTimestamp(schema);
        OracleTableInfo tableInfo = OracleTableUtils.createTableInfo(tableName, schema, props);
        OracleProperties properties=new OracleProperties(ORACLE_URL,ORACLE_USER_NAME,ORACLE_PASSWORD,ORACLE_SCHEMA);


        return new OracleTableSource(properties, tableInfo, schema, null, null);
    }

    @Override
    public OracleTableSink createTableSink(Map<String, String> properties) {
        DescriptorProperties descriptorProperties = getValidatedProps(properties);
        String tableName = descriptorProperties.getString(ORACLE_TABLE);
        TableSchema schema = descriptorProperties.getTableSchema(SCHEMA);

        return createTableSink(tableName, schema, properties);
    }

    @Override
    public OracleTableSink createTableSink(ObjectPath tablePath, CatalogTable table) {
        return createTableSink(tablePath.getObjectName(), table.getSchema(), table.getProperties());
    }

    private OracleTableSink createTableSink(String tableName, TableSchema schema, Map<String, String> props) {
        String masterAddresses = props.get(ORACLE_URL);
        TableSchema physicalSchema = OracleTableUtils.getSchemaWithSqlTimestamp(schema);
        OracleTableInfo tableInfo = OracleTableUtils.createTableInfo(tableName, schema, props);

        OracleProperties properties=new OracleProperties(props.get(ORACLE_URL),props.get(ORACLE_USER_NAME),props.get(ORACLE_PASSWORD),props.get(ORACLE_SCHEMA));
        return new OracleTableSink(properties, tableInfo, physicalSchema);
    }
}
