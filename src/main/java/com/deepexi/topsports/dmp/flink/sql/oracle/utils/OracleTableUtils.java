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

package com.deepexi.topsports.dmp.flink.sql.oracle.utils;


import com.deepexi.topsports.dmp.flink.sql.oracle.OracleTableFactory;
import com.deepexi.topsports.dmp.flink.sql.oracle.connector.*;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.expressions.CallExpression;
import org.apache.flink.table.expressions.Expression;
import org.apache.flink.table.expressions.FieldReferenceExpression;
import org.apache.flink.table.expressions.ValueLiteralExpression;
import org.apache.flink.table.functions.BuiltInFunctionDefinitions;
import org.apache.flink.table.functions.FunctionDefinition;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.TimestampType;
import org.apache.flink.table.utils.TableSchemaUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.sql.Timestamp;
import java.util.*;


public class OracleTableUtils {

    private static final Logger LOG = LoggerFactory.getLogger(OracleTableUtils.class);

    public static OracleTableInfo createTableInfo(String tableName, TableSchema schema, Map<String, String> props) {
        // Since KUDU_HASH_COLS is a required property for table creation, we use it to infer whether to create table
        //boolean createIfMissing = props.containsKey(OracleTableFactory.);

        OracleTableInfo tableInfo = OracleTableInfo.forTable(tableName);
       // ColumnSchemasFactory keyColumns = () -> ;
        return tableInfo.createTableIfNotExists(schema.getFieldNames(),getPrimaryKeyColumns(props, schema));
    }



    public static TableSchema oracleToFlinkSchema(OracleTable table) {
        TableSchema.Builder builder = TableSchema.builder();

        for (ColumnSchema column : table.getColumnSchema()) {
            DataType flinkType = OracleTypeUtils.toFlinkType(column.getType(), column.getTypeSize(), column.getDataScale()).nullable();
            builder.field(column.getName(), flinkType);
        }

        return builder.build();
    }

    public static String[] getPrimaryKeyColumns(Map<String, String> tableProperties, TableSchema tableSchema) {
        if (tableProperties.containsKey(OracleTableFactory.ORACLE_PRIMARY_KEY_COLS) || !tableSchema.getPrimaryKey().get().isEnforced())
         return     tableProperties.containsKey(OracleTableFactory.ORACLE_PRIMARY_KEY_COLS) ? tableProperties.get(OracleTableFactory.ORACLE_PRIMARY_KEY_COLS).split(",") : tableSchema.getPrimaryKey().get().getColumns().toArray(new String[tableSchema.getPrimaryKey().get().getColumns().size()]  );
        else
            return null;
    }

//    public static List<String> getHashColumns(Map<String, String> tableProperties) {
//        return Lists.newArrayList(tableProperties.get(KUDU_HASH_COLS).split(","));
//    }

    public static TableSchema getSchemaWithSqlTimestamp(TableSchema schema) {
        TableSchema.Builder builder = new TableSchema.Builder();
        TableSchemaUtils.getPhysicalSchema(schema).getTableColumns().forEach(
                tableColumn -> {
                    if (tableColumn.getType().getLogicalType() instanceof TimestampType) {
                        builder.field(tableColumn.getName(), tableColumn.getType().bridgedTo(Timestamp.class));
                    } else {
                        builder.field(tableColumn.getName(), tableColumn.getType());
                    }
                });
        return builder.build();
    }

    /**
     * Converts Flink Expression to OracleFilterInfo.
     */
    @Nullable
    public static Optional<OracleFilterInfo> toOracleFilterInfo(Expression predicate) {
        LOG.debug("predicate summary: [{}], class: [{}], children: [{}]",
                predicate.asSummaryString(), predicate.getClass(), predicate.getChildren());
        if (predicate instanceof CallExpression) {
            CallExpression callExpression = (CallExpression) predicate;
            FunctionDefinition functionDefinition = callExpression.getFunctionDefinition();
            List<Expression> children = callExpression.getChildren();
            if (children.size() == 1) {
                return convertUnaryIsNullExpression(functionDefinition, children);
            } else if (children.size() == 2 &&
                    !functionDefinition.equals(BuiltInFunctionDefinitions.OR)) {
                return convertBinaryComparison(functionDefinition, children);
            } else if (children.size() > 0 && functionDefinition.equals(BuiltInFunctionDefinitions.OR)) {
                return convertIsInExpression(children);
            }
        }
        return Optional.empty();
    }

    private static boolean isFieldReferenceExpression(Expression exp) {
        return exp instanceof FieldReferenceExpression;
    }

    private static boolean isValueLiteralExpression(Expression exp) {
        return exp instanceof ValueLiteralExpression;
    }

    private static Optional<OracleFilterInfo> convertUnaryIsNullExpression(
            FunctionDefinition functionDefinition, List<Expression> children) {
        FieldReferenceExpression fieldReferenceExpression;
        if (isFieldReferenceExpression(children.get(0))) {
            fieldReferenceExpression = (FieldReferenceExpression) children.get(0);
        } else {
            return Optional.empty();
        }
        // IS_NULL IS_NOT_NULL
        String columnName = fieldReferenceExpression.getName();
        OracleFilterInfo.Builder builder = OracleFilterInfo.Builder.create(columnName);
        if (functionDefinition.equals(BuiltInFunctionDefinitions.IS_NULL)) {
            return Optional.of(builder.isNull().build());
        } else if (functionDefinition.equals(BuiltInFunctionDefinitions.IS_NOT_NULL)) {
            return Optional.of(builder.isNotNull().build());
        }
        return Optional.empty();
    }

    private static Optional<OracleFilterInfo> convertBinaryComparison(
            FunctionDefinition functionDefinition, List<Expression> children) {
        FieldReferenceExpression fieldReferenceExpression;
        ValueLiteralExpression valueLiteralExpression;
        if (isFieldReferenceExpression(children.get(0)) &&
                isValueLiteralExpression(children.get(1))) {
            fieldReferenceExpression = (FieldReferenceExpression) children.get(0);
            valueLiteralExpression = (ValueLiteralExpression) children.get(1);
        } else if (isValueLiteralExpression(children.get(0)) &&
                isFieldReferenceExpression(children.get(1))) {
            fieldReferenceExpression = (FieldReferenceExpression) children.get(1);
            valueLiteralExpression = (ValueLiteralExpression) children.get(0);
        } else {
            return Optional.empty();
        }
        String columnName = fieldReferenceExpression.getName();
        Object value = extractValueLiteral(fieldReferenceExpression, valueLiteralExpression);
        if (value == null) {
            return Optional.empty();
        }
        OracleFilterInfo.Builder builder = OracleFilterInfo.Builder.create(columnName);
        // GREATER GREATER_EQUAL EQUAL LESS LESS_EQUAL
//        if (functionDefinition.equals(BuiltInFunctionDefinitions.GREATER_THAN)) {
//            return Optional.of(builder.greaterThan(value).build());
//        } else if (functionDefinition.equals(BuiltInFunctionDefinitions.GREATER_THAN_OR_EQUAL)) {
//            return Optional.of(builder.greaterOrEqualTo(value).build());
//        } else if (functionDefinition.equals(BuiltInFunctionDefinitions.EQUALS)) {
//            return Optional.of(builder.equalTo(value).build());
//        } else if (functionDefinition.equals(BuiltInFunctionDefinitions.LESS_THAN)) {
//            return Optional.of(builder.lessThan(value).build());
//        } else if (functionDefinition.equals(BuiltInFunctionDefinitions.LESS_THAN_OR_EQUAL)) {
//            return Optional.of(builder.lessOrEqualTo(value).build());
//        }
        return Optional.empty();
    }

    private static Optional<OracleFilterInfo> convertIsInExpression(List<Expression> children) {
        // IN operation will be: or(equals(field, value1), equals(field, value2), ...) in blink
        // For FilterType IS_IN, all internal CallExpression's function need to be equals and
        // fields need to be same
        List<Object> values = new ArrayList<>(children.size());
        String columnName = "";
        for (int i = 0; i < children.size(); i++) {
            if (children.get(i) instanceof CallExpression) {
                CallExpression callExpression = (CallExpression) children.get(i);
                FunctionDefinition functionDefinition = callExpression.getFunctionDefinition();
                List<Expression> subChildren = callExpression.getChildren();
                FieldReferenceExpression fieldReferenceExpression;
                ValueLiteralExpression valueLiteralExpression;
                if (functionDefinition.equals(BuiltInFunctionDefinitions.EQUALS) &&
                        subChildren.size() == 2 && isFieldReferenceExpression(subChildren.get(0)) &&
                        isValueLiteralExpression(subChildren.get(1))) {
                    fieldReferenceExpression = (FieldReferenceExpression) subChildren.get(0);
                    valueLiteralExpression = (ValueLiteralExpression) subChildren.get(1);
                    String fieldName = fieldReferenceExpression.getName();
                    if (i != 0 && !columnName.equals(fieldName)) {
                        return Optional.empty();
                    } else {
                        columnName = fieldName;
                    }
                    Object value = extractValueLiteral(fieldReferenceExpression,
                            valueLiteralExpression);
                    if (value == null) {
                        return Optional.empty();
                    }
                    values.add(i, value);
                } else {
                    return Optional.empty();
                }
            } else {
                return Optional.empty();
            }
        }
        OracleFilterInfo.Builder builder = OracleFilterInfo.Builder.create(columnName);
        return Optional.of(builder.isIn(values).build());
    }

    private static Object extractValueLiteral(FieldReferenceExpression fieldReferenceExpression,
                                              ValueLiteralExpression valueLiteralExpression) {
        DataType fieldType = fieldReferenceExpression.getOutputDataType();
        return valueLiteralExpression.getValueAs(fieldType.getConversionClass()).orElse(null);
    }
}
