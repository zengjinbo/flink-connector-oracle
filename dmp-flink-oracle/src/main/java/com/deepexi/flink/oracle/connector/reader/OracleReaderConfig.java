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
package com.deepexi.flink.oracle.connector.reader;

import com.deepexi.flink.oracle.config.OracleProperties;
import com.deepexi.flink.oracle.jdbc.OracleClient;
import org.apache.flink.annotation.PublicEvolving;

import java.io.Serializable;
import java.sql.SQLException;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Configuration used by {@link }. Specifies connection and other necessary properties.
 */
@PublicEvolving
public class OracleReaderConfig implements Serializable {

    private final OracleProperties oracleProperties;
    private final int rowLimit;

    private OracleReaderConfig(
            OracleProperties oracleProperties,
            int rowLimit) {

        this.oracleProperties = checkNotNull(oracleProperties, "Oracle masters cannot be null");
        this.rowLimit = checkNotNull(rowLimit, "Oracle rowLimit cannot be null");
    }

    public OracleProperties getOracleProperties() {
        return oracleProperties;
    }

    public int getRowLimit() {
        return rowLimit;
    }


    public OracleClient build() throws SQLException {
        return new OracleClient(oracleProperties);
    }

}
