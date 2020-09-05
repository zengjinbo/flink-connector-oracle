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
import com.deepexi.flink.oracle.jdbc.OracleClient;
import com.deepexi.flink.oracle.streaming.OracleSink;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.sinks.TableSink;
import org.apache.flink.table.sinks.UpsertStreamTableSink;
import org.apache.flink.table.utils.TableConnectorUtils;
import org.apache.flink.types.Row;

public class OracleTableSink implements UpsertStreamTableSink<Row> {

    private final OracleProperties properties;
    private final TableSchema flinkSchema;
    private final OracleTableInfo tableInfo;

    public OracleTableSink(OracleProperties properties, OracleTableInfo tableInfo, TableSchema flinkSchema) {
        this.properties = properties;
        this.tableInfo = tableInfo;
        this.flinkSchema = flinkSchema;
    }

    @Override
    public void setKeyFields(String[] keyFields) { /* this has no effect */}

    @Override
    public void setIsAppendOnly(Boolean isAppendOnly) { /* this has no effect */}

    @Override
    public TypeInformation<Row> getRecordType() {  return flinkSchema.toRowType(); }


    @Override
    public DataStreamSink<?> consumeDataStream(DataStream<Tuple2<Boolean, Row>> dataStreamTuple) {
        OracleSink upsertOracleSink = new OracleSink(properties, tableInfo, new UpsertOperationMapper(getTableSchema().getFieldNames()));

        return dataStreamTuple
                .addSink(upsertOracleSink)
                .setParallelism(dataStreamTuple.getParallelism())
                .name(TableConnectorUtils.generateRuntimeName(this.getClass(), getTableSchema().getFieldNames()));
    }

    @Override
    public TableSink<Tuple2<Boolean, Row>> configure(String[] fieldNames, TypeInformation<?>[] fieldTypes) {
        return new OracleTableSink(properties, tableInfo, flinkSchema);
    }

    @Override
    public TableSchema getTableSchema() { return flinkSchema; }
}
