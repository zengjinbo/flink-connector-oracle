/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.deepexi.topsports.dmp.flink.sql.oracle;


import com.deepexi.topsports.dmp.flink.sql.oracle.config.OracleProperties;
import com.deepexi.topsports.dmp.flink.sql.oracle.connector.ColumnSchema;
import com.deepexi.topsports.dmp.flink.sql.oracle.connector.OracleTable;
import com.deepexi.topsports.dmp.flink.sql.oracle.jdbc.OracleClient;
import com.deepexi.topsports.dmp.flink.sql.oracle.utils.OracleTableUtils;
import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.shaded.guava18.com.google.common.collect.Lists;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.catalog.*;
import org.apache.flink.table.catalog.exceptions.CatalogException;
import org.apache.flink.table.catalog.exceptions.DatabaseNotExistException;
import org.apache.flink.table.catalog.exceptions.FunctionNotExistException;
import org.apache.flink.table.catalog.exceptions.TableNotExistException;
import org.apache.flink.table.catalog.stats.CatalogColumnStatistics;
import org.apache.flink.table.catalog.stats.CatalogTableStatistics;
import org.apache.flink.table.expressions.Expression;
import org.apache.flink.table.factories.Factory;
import org.apache.flink.table.factories.TableFactory;
import org.apache.flink.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.util.*;
import java.util.stream.Collectors;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Catalog for reading and creating Oracle tables.
 */
@PublicEvolving
public class OracleCatalog extends AbstractReadOnlyCatalog {

    private static final Logger LOG = LoggerFactory.getLogger(OracleCatalog.class);
    private final OracleTableFactory tableFactory = new OracleTableFactory();
    private final OracleProperties properties;
    private OracleClient client;

    /**
     * Create a new {@link OracleCatalog} with the specified catalog name and kudu master addresses.
     *
     * @param catalogName Name of the catalog (used by the table environment)
     * @param properties Connection address to Oracle
     */
    public OracleCatalog(String catalogName,OracleProperties properties) {
        super(catalogName, EnvironmentSettings.DEFAULT_BUILTIN_DATABASE);
        this.properties = properties;
        this.client = createClient();
    }
    public OracleCatalog(String catalogName,String url, String userName, String password) {
        super(catalogName, EnvironmentSettings.DEFAULT_BUILTIN_DATABASE);
        this.properties = new OracleProperties(url,userName,password);
        this.client = createClient();
    }


    /**
     * Create a new {@link OracleCatalog} with the specified kudu master addresses.
     *
     * @param properties Connection address to Oracle
     */
    public OracleCatalog(OracleProperties properties) {
        this("oracle", properties);
    }
@Override
    public Optional<TableFactory> getTableFactory() {
        return Optional.of(getOracleTableFactory());
    }
@Override
public  Optional<Factory> getFactory()
{
    return Optional.of(new OracleFactory());
}
    public OracleTableFactory getOracleTableFactory() {
        return tableFactory;
    }

    private OracleClient createClient() {
        try {
            return new OracleClient(this.properties);
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        }
        return null;
    }

    @Override
    public void open() {}

    @Override
    public void close() {
 /*       try {
            if (kuduClient != null) {
                kuduClient.close();
            }
        } catch (OracleException e) {
            LOG.error("Error while closing kudu client", e);
        }*/
    }

    public ObjectPath getObjectPath(String tableName) {
        return new ObjectPath(getDefaultDatabase(), tableName);
    }

    @Override
    public List<String> listTables(String databaseName) throws DatabaseNotExistException, CatalogException {
        checkArgument(!StringUtils.isNullOrWhitespaceOnly(databaseName), "databaseName cannot be null or empty");

        if (!databaseExists(databaseName)) {
            throw new DatabaseNotExistException(getName(), databaseName);
        }

        try {
            return  null;// kuduClient.getTablesList().getTablesList();
        } catch (Throwable t) {
            throw new CatalogException("Could not list tables", t);
        }
    }

    @Override
    public boolean tableExists(ObjectPath tablePath) {
        checkNotNull(tablePath);
        try {
            return    client.tableExists(tablePath.getObjectName()) ;//kuduClient.tableExists(tablePath.getObjectName());
        } catch ( SQLException e) {
            throw new CatalogException(e);
        }
    }

    @Override
    public CatalogTable getTable(ObjectPath tablePath) throws TableNotExistException {
        checkNotNull(tablePath);

        if (!tableExists(tablePath)) {
            throw new TableNotExistException(getName(), tablePath);
        }

        String tableName = tablePath.getObjectName();

        try {
            OracleTable oracleTable = client.getTable(tableName);

            CatalogTableImpl table = new CatalogTableImpl(
                    OracleTableUtils.oracleToFlinkSchema(oracleTable),
                    createTableProperties(tableName, oracleTable.getColumnSchema()),
                    tableName);

            return table;
        } catch ( SQLException e) {
            throw new CatalogException(e);
        }
    }

    protected Map<String, String> createTableProperties(String tableName, List<ColumnSchema> primaryKeyColumns) {
        Map<String, String> props = new HashMap<>();
        props.put(OracleTableFactory.ORACLE_URL, this.properties.getUrl());
        props.put(OracleTableFactory.ORACLE_DRIVER, this.properties.getDriverClassName());
        props.put(OracleTableFactory.ORACLE_SCHEMA, this.properties.getSchema());
        props.put(OracleTableFactory.ORACLE_PASSWORD, this.properties.getPassword());
        props.put(OracleTableFactory.ORACLE_USER_NAME, this.properties.getUserName());
        String primaryKeyNames = primaryKeyColumns.stream().map(ColumnSchema::getName).collect(Collectors.joining(","));
        props.put(OracleTableFactory.ORACLE_PRIMARY_KEY_COLS, primaryKeyNames);
        props.put(OracleTableFactory.ORACLE_TABLE, tableName);
        return props;
    }

/*    @Override
    public void dropTable(ObjectPath tablePath, boolean ignoreIfNotExists) throws TableNotExistException {
        String tableName = tablePath.getObjectName();
        try {
            if (tableExists(tablePath)) {
                kuduClient.deleteTable(tableName);
            } else if (!ignoreIfNotExists) {
                throw new TableNotExistException(getName(), tablePath);
            }
        } catch (OracleException e) {
            throw new CatalogException("Could not delete table " + tableName, e);
        }
    }*/

/*    @Override
    public void renameTable(ObjectPath tablePath, String newTableName, boolean ignoreIfNotExists) throws TableNotExistException {
        String tableName = tablePath.getObjectName();
        try {
            if (tableExists(tablePath)) {
                client.alterTable(tableName, new AlterTableOptions().renameTable(newTableName));
            } else if (!ignoreIfNotExists) {
                throw new TableNotExistException(getName(), tablePath);
            }
        } catch (OracleException e) {
            throw new CatalogException("Could not rename table " + tableName, e);
        }
    }*/

/*    public void createTable(OracleTableInfo tableInfo, boolean ignoreIfExists) throws CatalogException, TableAlreadyExistException {
        ObjectPath path = getObjectPath(tableInfo.getName());
        if (tableExists(path)) {
            if (ignoreIfExists) {
                return;
            } else {
                throw new TableAlreadyExistException(getName(), path);
            }
        }

        try {
            kuduClient.createTable(tableInfo.getName(), tableInfo.getSchema(), tableInfo.getCreateTableOptions());
        } catch (
                OracleException e) {
            throw new CatalogException("Could not create table " + tableInfo.getName(), e);
        }
    }*/

/*
    @Override
    public void createTable(ObjectPath tablePath, CatalogBaseTable table, boolean ignoreIfExists) throws TableAlreadyExistException {
        Map<String, String> tableProperties = table.getProperties();
        TableSchema tableSchema = table.getSchema();

        Set<String> optionalProperties = new HashSet<>(Arrays.asList(KUDU_REPLICAS));
        Set<String> requiredProperties = new HashSet<>(Arrays.asList(KUDU_HASH_COLS));

        if (!tableSchema.getPrimaryKey().isPresent()) {
            requiredProperties.add(KUDU_PRIMARY_KEY_COLS);
        }

        if (!tableProperties.keySet().containsAll(requiredProperties)) {
            throw new CatalogException("Missing required property. The following properties must be provided: " +
                    requiredProperties.toString());
        }

        Set<String> permittedProperties = Sets.union(requiredProperties, optionalProperties);
        if (!permittedProperties.containsAll(tableProperties.keySet())) {
            throw new CatalogException("Unpermitted properties were given. The following properties are allowed:" +
                    permittedProperties.toString());
        }

        String tableName = tablePath.getObjectName();

        OracleTableInfo tableInfo = OracleTableUtils.createTableInfo(tableName, tableSchema, tableProperties);

        createTable(tableInfo, ignoreIfExists);
    }
*/

    @Override
    public CatalogTableStatistics getTableStatistics(ObjectPath tablePath) throws CatalogException {
        return CatalogTableStatistics.UNKNOWN;
    }

    @Override
    public CatalogColumnStatistics getTableColumnStatistics(ObjectPath tablePath) throws CatalogException {
        return CatalogColumnStatistics.UNKNOWN;
    }

    @Override
    public CatalogTableStatistics getPartitionStatistics(ObjectPath tablePath, CatalogPartitionSpec partitionSpec) throws CatalogException {
        return CatalogTableStatistics.UNKNOWN;
    }

    @Override
    public CatalogColumnStatistics getPartitionColumnStatistics(ObjectPath tablePath, CatalogPartitionSpec partitionSpec) throws CatalogException {
        return CatalogColumnStatistics.UNKNOWN;
    }

    @Override
    public List<String> listDatabases() throws CatalogException {
        return Lists.newArrayList(getDefaultDatabase());
    }

    @Override
    public CatalogDatabase getDatabase(String databaseName) throws DatabaseNotExistException, CatalogException {
        if (databaseName.equals(getDefaultDatabase())) {
            return new CatalogDatabaseImpl(new HashMap<>(), null);
        } else {
            throw new DatabaseNotExistException(getName(), databaseName);
        }
    }

    @Override
    public boolean databaseExists(String databaseName) throws CatalogException {
        return listDatabases().contains(databaseName);
    }

    @Override
    public List<String> listViews(String databaseName) throws CatalogException {
        return Collections.emptyList();
    }

    @Override
    public List<CatalogPartitionSpec> listPartitions(ObjectPath tablePath) throws CatalogException {
        return Collections.emptyList();
    }

    @Override
    public List<CatalogPartitionSpec> listPartitions(ObjectPath tablePath, CatalogPartitionSpec partitionSpec) throws CatalogException {
        return Collections.emptyList();
    }

    @Override
    public List<CatalogPartitionSpec> listPartitionsByFilter(ObjectPath tablePath, List<Expression> filters) throws CatalogException {
        return Collections.emptyList();
    }

    @Override
    public boolean partitionExists(ObjectPath tablePath, CatalogPartitionSpec partitionSpec) throws CatalogException {
        return false;
    }

    @Override
    public List<String> listFunctions(String dbName) throws CatalogException {
        return Collections.emptyList();
    }

    @Override
    public CatalogFunction getFunction(ObjectPath functionPath) throws FunctionNotExistException, CatalogException {
        throw new FunctionNotExistException(getName(), functionPath);
    }

    @Override
    public boolean functionExists(ObjectPath functionPath) throws CatalogException {
        return false;
    }

}
