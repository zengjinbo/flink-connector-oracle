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
import com.deepexi.flink.oracle.connector.OracleFilterInfo;
import com.deepexi.flink.oracle.connector.OracleTableInfo;
import com.deepexi.flink.oracle.jdbc.OracleClient;
import com.deepexi.flink.oracle.utils.OracleTableUtils;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.shaded.guava18.com.google.common.collect.Lists;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.expressions.Expression;
import org.apache.flink.table.sources.*;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.utils.TypeConversions;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.util.*;


public class OracleTableSource implements StreamTableSource<Row>,
    LimitableTableSource<Row>, ProjectableTableSource<Row>, FilterableTableSource<Row> {

    private static final Logger LOG = LoggerFactory.getLogger(OracleTableSource.class);

    private final OracleProperties properties;;
    private final OracleTableInfo tableInfo;
    private final TableSchema flinkSchema;
    private final String[] projectedFields;
    // predicate expression to apply
    @Nullable
    private final List<OracleFilterInfo> predicates;
    private boolean isFilterPushedDown;

  //  private OracleRowInputFormat kuduRowInputFormat;

    public OracleTableSource(OracleProperties properties, OracleTableInfo tableInfo,
                             TableSchema flinkSchema, List<OracleFilterInfo> predicates, String[] projectedFields) {
        this.properties = properties;
        this.tableInfo = tableInfo;
        this.flinkSchema = flinkSchema;
        this.predicates = predicates;
        this.projectedFields = projectedFields;
        if (predicates != null && predicates.size() != 0) {
            this.isFilterPushedDown = true;
        }
//        this.kuduRowInputFormat = new OracleRowInputFormat(oracleClient, tableInfo,
//            predicates == null ? Collections.emptyList() : predicates,
//            projectedFields == null ? null : Lists.newArrayList(projectedFields));
    }

    @Override
    public boolean isBounded() {
        return true;
    }

    @Override
    public DataStream<Row> getDataStream(StreamExecutionEnvironment env) {
//        OracleRowInputFormat inputFormat = new OracleRowInputFormat(oracleClient, tableInfo,
//            predicates == null ? Collections.emptyList() : predicates,
//            projectedFields == null ? null : Lists.newArrayList(projectedFields));
        return env.createInput(null,
            (TypeInformation<Row>) TypeConversions.fromDataTypeToLegacyInfo(getProducedDataType()))
            .name(explainSource());
    }

    @Override
    public TableSchema getTableSchema() {
        return flinkSchema;
    }

    @Override
    public boolean isFilterPushedDown() {
        return this.isFilterPushedDown;
    }

    @Override
    public DataType getProducedDataType() {
        if (projectedFields == null) {
            return flinkSchema.toRowDataType();
        } else {
            DataTypes.Field[] fields = new DataTypes.Field[projectedFields.length];
            for (int i = 0; i < fields.length; i++) {
                String fieldName = projectedFields[i];
                fields[i] = DataTypes.FIELD(
                        fieldName,
                        flinkSchema
                                .getTableColumn(fieldName)
                                .get()
                                .getType()
                );
            }
            return DataTypes.ROW(fields);
        }
    }

    @Override
    public boolean isLimitPushedDown() {
        return true;
    }

    @Override
    public TableSource<Row> applyLimit(long l) {
        return new OracleTableSource(properties, tableInfo, flinkSchema,
            predicates, projectedFields);
    }

    @Override
    public TableSource<Row> projectFields(int[] ints) {
        String[] fieldNames = new String[ints.length];
        RowType producedDataType = (RowType) getProducedDataType().getLogicalType();
        List<String> prevFieldNames = producedDataType.getFieldNames();
        for (int i = 0; i < ints.length; i++) {
            fieldNames[i] = prevFieldNames.get(ints[i]);
        }
        return new OracleTableSource(properties, tableInfo, flinkSchema, predicates, fieldNames);
    }

    @Override
    public TableSource<Row> applyPredicate(List<Expression> predicates) {
        List<OracleFilterInfo> kuduPredicates = new ArrayList<>();
        ListIterator<Expression> predicatesIter = predicates.listIterator();
        while(predicatesIter.hasNext()) {
            Expression predicate = predicatesIter.next();
            Optional<OracleFilterInfo> oraclePred = OracleTableUtils.toOracleFilterInfo(predicate);
            if (oraclePred != null && oraclePred.isPresent()) {
                LOG.debug("Predicate [{}] converted into OracleFilterInfo and pushed into " +
                    "OracleTable [{}].", predicate, tableInfo.getName());
                kuduPredicates.add(oraclePred.get());
                predicatesIter.remove();
            } else {
                LOG.debug("Predicate [{}] could not be pushed into OracleFilterInfo for OracleTable [{}].",
                    predicate, tableInfo.getName());
            }
        }
        return new OracleTableSource(properties, tableInfo, flinkSchema, kuduPredicates, projectedFields);
    }

    @Override
    public String explainSource() {
        return "OracleTableSource[schema=" + Arrays.toString(getTableSchema().getFieldNames()) +
            ", filter=" + predicateString() +
            (projectedFields != null ?", projectFields=" + Arrays.toString(projectedFields) + "]" : "]");
    }

    private String predicateString() {
        if (predicates == null || predicates.size() == 0) {
            return "No predicates push down";
        } else {
            return "AND(" + predicates + ")";
        }
    }
}
