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
package com.deepexi.topsports.dmp.flink.sql.oracle.connector.reader;

import com.deepexi.topsports.dmp.flink.sql.oracle.connector.OracleFilterInfo;
import com.deepexi.topsports.dmp.flink.sql.oracle.connector.OracleTable;
import com.deepexi.topsports.dmp.flink.sql.oracle.connector.OracleTableInfo;
import com.deepexi.topsports.dmp.flink.sql.oracle.jdbc.OracleClient;
import org.apache.flink.annotation.Internal;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

@Internal
public class OracleReader implements AutoCloseable {

    private final Logger log = LoggerFactory.getLogger(getClass());

    private final OracleTableInfo tableInfo;
    private final OracleReaderConfig readerConfig;
    private final List<OracleFilterInfo> tableFilters;
    private final List<String> tableProjections;

    private transient OracleClient client;
    private transient OracleTable table;

    public OracleReader(OracleTableInfo tableInfo, OracleReaderConfig readerConfig) throws IOException {
        this(tableInfo, readerConfig, new ArrayList<>(), null);
    }

    public OracleReader(OracleTableInfo tableInfo, OracleReaderConfig readerConfig, List<OracleFilterInfo> tableFilters) throws IOException {
        this(tableInfo, readerConfig, tableFilters, null);
    }

    public OracleReader(OracleTableInfo tableInfo, OracleReaderConfig readerConfig, List<OracleFilterInfo> tableFilters, List<String> tableProjections) throws IOException {
        this.tableInfo = tableInfo;
        this.readerConfig = readerConfig;
        this.tableFilters = tableFilters;
        this.tableProjections = tableProjections;

        this.client = obtainClient();
        this.table = obtainTable();
    }

    private OracleClient obtainClient() {
        try {
            return    readerConfig.build();// new OracleClient.OracleClientBuilder(readerConfig.getOracleProperties()).build();
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        }
        return null;
    }


    private OracleTable obtainTable()  {
        String tableName = tableInfo.getName();
        try {
            if (client.tableExists(tableName)) {
                return client.getTable(tableName);
            }else
                //markCanceledOrStopped();
                throw new UnsupportedOperationException("table not exists and is marketed to not be created");

        } catch (SQLException throwables) {
            throwables.printStackTrace();
        }

        throw new UnsupportedOperationException("table not exists and is marketed to not be created");
    }

    /*public OracleReaderIterator scanner(byte[] token) throws IOException {
        return new OracleReaderIterator(OracleScanToken.deserializeIntoScanner(token, client));
    }*/

/*    public List<OracleScanToken> scanTokens(List<OracleFilterInfo> tableFilters, List<String> tableProjections, Integer rowLimit) {
        OracleScanToken.OracleScanTokenBuilder tokenBuilder = client.newScanTokenBuilder(table);

        if (tableProjections != null) {
            tokenBuilder.setProjectedColumnNames(tableProjections);
        }

        if (CollectionUtils.isNotEmpty(tableFilters)) {
            tableFilters.stream()
                    .map(filter -> filter.toPredicate(table.getSchema()))
                    .forEach(tokenBuilder::addPredicate);
        }

        if (rowLimit != null && rowLimit > 0) {
            tokenBuilder.limit(rowLimit);
        }

        return tokenBuilder.build();
    }

    public OracleInputSplit[] createInputSplits(int minNumSplits) throws IOException {

     //   List<OracleScanToken> tokens = scanTokens(tableFilters, tableProjections, readerConfig.getRowLimit());

        OracleInputSplit[] splits = new OracleInputSplit[tokens.size()];

        for (int i = 0; i < tokens.size(); i++) {
            OracleScanToken token = tokens.get(i);

            List<String> locations = new ArrayList<>(token.getTablet().getReplicas().size());

            for (LocatedTablet.Replica replica : token.getTablet().getReplicas()) {
                locations.add(getLocation(replica.getRpcHost(), replica.getRpcPort()));
            }

            OracleInputSplit split = new OracleInputSplit(
                    token.serialize(),
                    i,
                    locations.toArray(new String[locations.size()])
            );
            splits[i] = split;
        }

        if (splits.length < minNumSplits) {
            log.warn(" The minimum desired number of splits with your configured parallelism level " +
                            "is {}. Current kudu splits = {}. {} instances will remain idle.",
                    minNumSplits,
                    splits.length,
                    (minNumSplits - splits.length)
            );
        }

        return splits;
    }*/

    /**
     * Returns a endpoint url in the following format: <host>:<ip>
     *
     * @param host Hostname
     * @param port Port
     * @return Formatted URL
     */
    private String getLocation(String host, Integer port) {
        StringBuilder builder = new StringBuilder();
        builder.append(host).append(":").append(port);
        return builder.toString();
    }

    @Override
    public void close() throws IOException {

        try {
            if (client != null) {
                client.close();
            }
        } catch (Exception e) {
            log.error("Error while closing client.", e);
        }
    }
}
