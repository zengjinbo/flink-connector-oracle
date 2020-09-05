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
package com.deepexi.flink.oracle.streaming;


import com.deepexi.flink.oracle.config.OracleProperties;
import com.deepexi.flink.oracle.connector.OracleTableInfo;
import com.deepexi.flink.oracle.connector.failure.DefaultOracleFailureHandler;
import com.deepexi.flink.oracle.connector.failure.OracleFailureHandler;
import com.deepexi.flink.oracle.connector.writer.OracleOperationMapper;
import com.deepexi.flink.oracle.connector.writer.OracleWriter;
import com.deepexi.flink.oracle.connector.writer.OracleWriterConfig;
import com.deepexi.flink.oracle.jdbc.OracleClient;
import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Streaming Sink that executes Oracle operations based on the incoming elements.
 * The target Oracle table is defined in the {@link OracleTableInfo} object together with parameters for table
 * creation in case the table does not exist.
 * <p>
 * Incoming records are mapped to Oracle table operations using the provided {@link OracleOperationMapper} logic. While
 * failures resulting from the operations are handled by the {@link OracleFailureHandler} instance.
 *
 * @param <IN> Type of the input records
 */
@PublicEvolving
public class OracleSink<IN> extends RichSinkFunction<IN> implements CheckpointedFunction {

    private final Logger log = LoggerFactory.getLogger(getClass());

    private final OracleTableInfo tableInfo;
    private final OracleProperties properties;
    private final OracleFailureHandler failureHandler;
    private final OracleOperationMapper<IN> opsMapper;
    private transient OracleWriter kuduWriter;

    /**
     * Creates a new {@link OracleSink} that will execute operations against the specified Oracle table (defined in {@link OracleTableInfo})
     * for the incoming stream elements.
     *
     * @param properties Writer configuration
     * @param tableInfo    Table information for the target table
     * @param opsMapper    Mapping logic from inputs to Oracle operations
     */
    public OracleSink(OracleProperties properties, OracleTableInfo tableInfo, OracleOperationMapper<IN> opsMapper) {
        this(properties, tableInfo, opsMapper, new DefaultOracleFailureHandler());
    }

    /**
     * Creates a new {@link OracleSink} that will execute operations against the specified Oracle table (defined in {@link OracleTableInfo})
     * for the incoming stream elements.
     *
     * @param properties   Writer configuration
     * @param tableInfo      Table information for the target table
     * @param opsMapper      Mapping logic from inputs to Oracle operations
     * @param failureHandler Custom failure handler instance
     */
    public OracleSink(OracleProperties properties, OracleTableInfo tableInfo, OracleOperationMapper<IN> opsMapper, OracleFailureHandler failureHandler) {
        this.tableInfo = checkNotNull(tableInfo, "tableInfo could not be null");
        this.properties = checkNotNull(properties, "config could not be null");
        this.opsMapper = checkNotNull(opsMapper, "opsMapper could not be null");
        this.failureHandler = checkNotNull(failureHandler, "failureHandler could not be null");
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        kuduWriter = new OracleWriter(tableInfo, properties, opsMapper, failureHandler);
    }
    @Override
    public void invoke(IN value) throws Exception {
        try {
            kuduWriter.write(value);
        } catch (ClassCastException e) {
            failureHandler.onTypeMismatch(e);
        }
    }

    @Override
    public void close() throws Exception {
        if (kuduWriter != null) {
            kuduWriter.close();
        }
    }

    @Override
    public void snapshotState(FunctionSnapshotContext functionSnapshotContext) throws Exception {
        kuduWriter.flushAndCheckErrors();
    }

    @Override
    public void initializeState(FunctionInitializationContext functionInitializationContext) throws Exception {
    }

}
