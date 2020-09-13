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


import com.deepexi.topsports.dmp.flink.sql.oracle.config.OracleProperties;
import com.deepexi.topsports.dmp.flink.sql.oracle.connector.OracleTable;
import com.deepexi.topsports.dmp.flink.sql.oracle.connector.OracleTableInfo;
import com.deepexi.topsports.dmp.flink.sql.oracle.connector.failure.DefaultOracleFailureHandler;
import com.deepexi.topsports.dmp.flink.sql.oracle.connector.failure.OracleFailureHandler;
import com.deepexi.topsports.dmp.flink.sql.oracle.jdbc.OracleClient;
import com.deepexi.topsports.dmp.flink.sql.oracle.utils.TableUtils;
import org.apache.flink.annotation.Internal;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.SQLException;
import java.util.Optional;

@Internal
public class OracleWriter<T> implements AutoCloseable {

    private final Logger log = LoggerFactory.getLogger(getClass());

    private final OracleTableInfo tableInfo;
    private final OracleProperties properties;
    private final OracleFailureHandler failureHandler;
    private final OracleOperationMapper<T> operationMapper;

    private transient OracleClient client;
    private transient OracleTable table;

    public OracleWriter(OracleTableInfo tableInfo, OracleProperties properties, OracleOperationMapper<T> operationMapper) throws IOException {
        this(tableInfo, properties, operationMapper, new DefaultOracleFailureHandler());
    }

    public OracleWriter(OracleTableInfo tableInfo, OracleProperties properties, OracleOperationMapper<T> operationMapper, OracleFailureHandler failureHandler) throws IOException {
        this.tableInfo = tableInfo;
        this.properties = properties;
        this.failureHandler = failureHandler;

        this.client = obtainClient();
        this.table = obtainTable();
        this.operationMapper = operationMapper;
    }

    private OracleClient obtainClient() {
        try {
            return new OracleClient(properties);
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        }
        return  null;
    }



    private OracleTable obtainTable()  {
        String tableName = tableInfo.getName();
        try {
            if (client.tableExists(tableName)) {
                return client.getTable(tableName);
            } else
                throw new UnsupportedOperationException("table not exists and is marketed to not be created");

        } catch (SQLException throwables) {
            throwables.printStackTrace();
        }

        throw new UnsupportedOperationException("table not exists and is marketed to not be created");
    }

    public void write(T input) throws IOException {
        //checkAsyncErrors();
       // operationMapper.createOperations(input, table);
     log.debug(input.toString());
        System.out.println(input.toString());
             try {
             Optional<String> operationOpt = operationMapper.createBaseOperation(input, table);

               if (operationOpt.get().equalsIgnoreCase("insert")&&tableInfo.getKeyColumns()!=null)
                {

                   // Optional<String> optionalS=  operationMapper.getUpsertStatement(table);
                    client.mergeInto(table.getSchema(),table.getName(),tableInfo.getKeyColumns(),operationMapper.createOperations(input, table).get(0));
                }
                else if (operationOpt.get().equalsIgnoreCase("insert"))
                    client.insertAll(TableUtils.getTableFullPath(table.getSchema(), table.getName()),operationMapper.createOperations(input, table));
                else if (operationOpt.get().equalsIgnoreCase("delete"))
                {
                    client.delete(table.getName(),operationMapper.createOperations(input, table).get(0));
                }



            } catch (SQLException throwables) {
                throwables.printStackTrace();
            }

    }

    public void flushAndCheckErrors() throws IOException {
       // checkAsyncErrors();
        flush();
      //  checkAsyncErrors();
    }

/*    @VisibleForTesting
    public DeleteTableResponse deleteTable() throws IOException {
        String tableName = table.getName();
        return client.deleteTable(tableName);
    }*/

    @Override
    public void close() throws IOException {
        try {
            flushAndCheckErrors();
        } finally {

//            try {
//                if (client != null) {
//                    client.close();
//                }
//            } catch (Exception e) {
//                log.error("Error while closing client.", e);
//            }
        }
    }

    private void flush() throws IOException {
       // session.flush();
    }

/*    private void checkErrors(OperationResponse response) throws IOException {
        if (response != null && response.hasRowError()) {
            failureHandler.onFailure(Arrays.asList(response.getRowError()));
        } else {
            checkAsyncErrors();
        }
    }*/

/*    private void checkAsyncErrors() throws IOException {
        if (session.countPendingErrors() == 0) { return; }

        List<RowError> errors = Arrays.asList(session.getPendingErrors().getRowErrors());
        failureHandler.onFailure(errors);
    }*/
}
