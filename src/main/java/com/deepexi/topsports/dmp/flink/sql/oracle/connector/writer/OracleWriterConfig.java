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
package com.deepexi.topsports.dmp.flink.sql.oracle.connector.writer;

import com.deepexi.topsports.dmp.flink.sql.oracle.batch.OracleOutputFormat;
import com.deepexi.topsports.dmp.flink.sql.oracle.config.OracleProperties;
import com.deepexi.topsports.dmp.flink.sql.oracle.jdbc.OracleClient;
import com.deepexi.topsports.dmp.flink.sql.oracle.streaming.OracleSink;
import org.apache.flink.annotation.PublicEvolving;

import java.io.Serializable;
import java.sql.SQLException;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Configuration used by {@link OracleSink} and {@link OracleOutputFormat}.
 * Specifies connection and other necessary properties.
 */
@PublicEvolving
public class OracleWriterConfig implements Serializable {

    private final OracleProperties properties;

    private OracleWriterConfig(
            OracleProperties properties) {

        this.properties = checkNotNull(properties, "Oracle properties cannot be null");
    }

    public OracleProperties getProperties() {
        return properties;
    }

        public OracleClient build() throws SQLException {
            return new OracleClient(this.properties);
        }
    }

