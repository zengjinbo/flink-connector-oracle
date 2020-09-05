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
import org.apache.commons.collections.map.CaseInsensitiveMap;
import org.apache.flink.annotation.PublicEvolving;

import java.util.*;

/**
 * Base implementation for {@link OracleOperationMapper}s that have one-to-one input to
 * Oracle operation mapping. It requires a fixed table schema to be provided at construction
 * time and only requires users to implement a getter for a specific column index (relative
 * to the ones provided in the constructor).
 * <br>
 * Supports both fixed operation type per record by specifying the {@link OracleOperation} or a
 * custom implementation for creating the base {@link } throwugh the
 * {@link #createBaseOperation(Object, OracleTable)} method.
 *
 * @param <T> Input type
 */
@PublicEvolving
public abstract class AbstractSingleOperationMapper<T> implements OracleOperationMapper<T> {

    protected final String[] columnNames;
    private final OracleOperation operation;

    protected AbstractSingleOperationMapper(String[] columnNames) {
        this(columnNames, null);
    }

    public AbstractSingleOperationMapper(String[] columnNames, OracleOperation operation) {
        this.columnNames = columnNames;
        this.operation = operation;
    }

    /**
     * Returns the object corresponding to the given column index.
     *
     * @param input Input element
     * @param i     Column index
     * @return Column value
     */
    public abstract Object getField(T input, int i);

    public Optional<String> createBaseOperation(T input, OracleTable table) {
        if (operation == null) {
            throw new UnsupportedOperationException("createBaseOperation must be overridden if no operation specified in constructor");
        }
        switch (operation) {
            case INSERT:
                return Optional.of("insert into ");
            case UPDATE:
                return Optional.of("update");
            case UPSERT:
                return Optional.of("merge into");
            case DELETE:
                return Optional.of("delete from");
            default:
                throw new RuntimeException("Unknown operation " + operation);
        }
    }

    @Override
    public List<Map<String, Object>>  createOperations(T input, OracleTable table) {
        Optional<String> operationOpt = createBaseOperation(input, table);

        List<Map<String, Object>> list=new ArrayList<>();
        if (!operationOpt.isPresent()) {
            return Collections.emptyList();
        }

            list.add(getMap(input));



        return list;
    }

    public enum OracleOperation {
        INSERT,
        UPDATE,
        UPSERT,
        DELETE
    }

    private Map<String, Object> getMap(T input)
    {
        Map<String, Object> map=new CaseInsensitiveMap();
        for (int i = 0; i < columnNames.length; i++) {
            //  partialRow.addObject(columnNames[i], getField(input, i));
            map.put(columnNames[i],getField(input, i));
        }
        return map;
    }




}
