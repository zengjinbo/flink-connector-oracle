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
package com.deepexi.flink.oracle.connector.writer;

import com.deepexi.flink.oracle.connector.OracleTable;
import org.apache.flink.annotation.PublicEvolving;


import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Encapsulates the logic of mapping input records (of a DataStream) to operations
 * executed in Oracle. By allowing to return a list of operations we give flexibility
 * to the implementers to provide more sophisticated logic.
 *
 * @param <T> Type of the input data
 */
@PublicEvolving
public interface OracleOperationMapper<T> extends Serializable {

    /**
     * Create a list of operations to be executed by the {@link OracleWriter} for the
     * current input
     *
     * @param input input element
     * @param table table for which the operations should be created
     * @return List of operations to be executed on the table
     */
    List<Map<String, Object>>  createOperations(T input, OracleTable table);

    Optional<String> createBaseOperation(T input, OracleTable table);

}
