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
package com.deepexi.flink.oracle.connector;

import org.apache.commons.lang3.Validate;
import org.apache.flink.annotation.PublicEvolving;

import java.io.Serializable;

/**
 * Describes which table should be used in sources and sinks along with specifications
 * on how to create it if it does not exist.
 *
 * <p> For sources and sinks reading from already existing tables, simply use @{@link OracleTableInfo#forTable(String)}
 * and if you want the system to create the table if it does not exist you need to specify the column and options
 * factories through {@link OracleTableInfo#createTableIfNotExists}
 */
@PublicEvolving
public class OracleTableInfo implements Serializable {

    private String name;
    private String[] keyColumns;
    private String[] columnSchema;
    private OracleTableInfo(String name) {
        this.name = Validate.notNull(name);
    }

    /**
     * Creates a new {@link OracleTableInfo} that is sufficient for reading/writing to existing Oracle Tables.
     *
     * @param name Table name in Oracle
     * @return OracleTableInfo for the given table name
     */
    public static OracleTableInfo forTable(String name) {
        return new OracleTableInfo(name);
    }

    public OracleTableInfo createTableIfNotExists(String[] columnSchema, String[] keyColumns) {
        this.keyColumns = Validate.notNull(keyColumns);
        this.columnSchema = Validate.notNull(columnSchema);
        return this;
    }
    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String[]  getKeyColumns() {
        return keyColumns;
    }

    public String[] getColumnSchema() {
        return columnSchema;
    }

}
