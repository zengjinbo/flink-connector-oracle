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
package com.deepexi.flink.oracle.batch;


import com.deepexi.flink.oracle.config.OracleProperties;
import com.deepexi.flink.oracle.connector.OracleTableInfo;
import com.deepexi.flink.oracle.connector.failure.DefaultOracleFailureHandler;
import com.deepexi.flink.oracle.connector.failure.OracleFailureHandler;
import com.deepexi.flink.oracle.connector.writer.OracleOperationMapper;
import com.deepexi.flink.oracle.connector.writer.OracleWriter;
import com.deepexi.flink.oracle.connector.writer.OracleWriterConfig;
import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.io.RichOutputFormat;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Output format for writing data into a Oracle table (defined by the provided {@link OracleTableInfo}) in both batch
 * and stream programs.
 */
@PublicEvolving
public class OracleOutputFormat<IN> extends RichOutputFormat<IN> implements CheckpointedFunction {

    private final Logger log = LoggerFactory.getLogger(getClass());

    private final OracleTableInfo tableInfo;
    private final OracleProperties properties;
    private final OracleFailureHandler failureHandler;
    private final OracleOperationMapper<IN> opsMapper;

    private transient OracleWriter writer;

    public OracleOutputFormat(OracleProperties properties, OracleTableInfo tableInfo, OracleOperationMapper<IN> opsMapper) {
        this(properties, tableInfo, opsMapper, new DefaultOracleFailureHandler());
    }

    public OracleOutputFormat(OracleProperties properties, OracleTableInfo tableInfo, OracleOperationMapper<IN> opsMapper, OracleFailureHandler failureHandler) {
        this.tableInfo = checkNotNull(tableInfo, "tableInfo could not be null");
        this.properties = checkNotNull(properties, "config could not be null");
        this.opsMapper = checkNotNull(opsMapper, "opsMapper could not be null");
        this.failureHandler = checkNotNull(failureHandler, "failureHandler could not be null");
    }

    @Override
    public void configure(Configuration parameters) {
    }

    @Override
    public void open(int taskNumber, int numTasks) throws IOException {
        writer = new OracleWriter(tableInfo, properties, opsMapper, failureHandler);
    }

    @Override
    public void writeRecord(IN row) throws IOException {
        writer.write(row);
    }

    @Override
    public void close() throws IOException {
        if (writer != null) {
            writer.close();
        }
    }

    @Override
    public void snapshotState(FunctionSnapshotContext functionSnapshotContext) throws Exception {
        writer.flushAndCheckErrors();
    }

    @Override
    public void initializeState(FunctionInitializationContext functionInitializationContext) throws Exception {

    }
}
