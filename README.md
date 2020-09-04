# Flink Oracle Connector

This connector provides a source (```OracleInputFormat```), a sink/output
(```OracleSink``` and ```OracleOutputFormat```, respectively),
 as well a table source (`OracleTableSource`), an upsert table sink (`OracleTableSink`), and a catalog (`OracleCatalog`),
 to allow reading and writing to [Oracle](https://oracle.apache.org/).

To use this connector, add the following dependency to your project:


 *Version Compatibility*: This module is compatible with Apache Oracle *1.11.1* (last stable version) and Apache Flink 1.10.+.

Note that the streaming connectors are not part of the binary distribution of Flink. You need to link them into your job jar for cluster execution.
See how to link with them for cluster execution [here](https://ci.apache.org/projects/flink/flink-docs-release-1.10/dev/projectsetup/dependencies.html).

## Installing Oracle

Follow the instructions from the [Oracle Installation Guide](https://oracle.apache.org/docs/installation.html).
Optionally, you can use the docker images provided in dockers folder.

## SQL and Table API

The Oracle connector is fully integrated with the Flink Table and SQL APIs. Once we configure the Oracle catalog (see next section)
we can start querying or inserting into existing Oracle tables using the Flink SQL or Table API.

For more information about the possible queries please check the [official documentation](https://ci.apache.org/projects/flink/flink-docs-release-1.10/dev/table/sql/)

### Oracle Catalog

The connector comes with a catalog implementation to handle metadata about your Oracle setup and perform table management.
By using the Oracle catalog, you can access all the tables already created in Oracle from Flink SQL queries. The Oracle catalog only
allows users to create or access existing Oracle tables. Tables using other data sources must be defined in other catalogs such as
in-memory catalog or Hive catalog.

When using the SQL CLI you can easily add the Oracle catalog to your environment yaml file:

```
flink-sql:
  oracle:
    servers:
      url: jdbc:oracle:thin:@127.0.0.1:1521:dmpdb
      classname: oracle.jdbc.OracleDriver
      username: oracle
      password: oracle
```

Once the SQL CLI is started you can simply switch to the Oracle catalog by calling `USE CATALOG oracle;`

You can also create and use the OracleCatalog directly in the Table environment:

```java
        OracleProperties oracleProperties=new OracleProperties();
        oracleProperties.setUrl("jdbc:oracle:thin:@127.0.0.1:1521:dmpdb");
        oracleProperties.setDriverClassName("oracle.jdbc.OracleDriver");
        oracleProperties.setUserName("oracle");
        oracleProperties.setPassword("oracle");
        OracleCatalog oracleCatalog = new OracleCatalog("oracle",oracleProperties);
        tableEnv.registerCatalog("oracle",oracleCatalog);
```

### DDL operations using SQL

It is possible to manipulate Oracle tables using SQL DDL.

When not using the Oracle catalog, the following additional properties must be specified in the `WITH` clause:
* `'connector.type'='oracle'`
* `'oracle.masters'='host1:port1,host2:port2,...'`: comma-delimitered list of Oracle masters
* `'oracle.table'='...'`: The table's name within the Oracle database.

If you have registered and are using the Oracle catalog, these properties are handled automatically.

To create a table, the additional properties `oracle.primary-key-columns` and `oracle.hash-columns` must be specified
as comma-delimited lists. Optionally, you can set the `oracle.replicas` property (defaults to 1).
Other properties, such as range partitioning, cannot be configured here - for more flexibility, please use
`catalog.createTable` as described in [this](#Creating-a-OracleTable-directly-with-OracleCatalog) section or create the table directly in Oracle.

The `NOT NULL` constraint can be added to any of the column definitions.
By setting a column as a primary key, it will automatically by created with the `NOT NULL` constraint.
Hash columns must be a subset of primary key columns.

Oracle Catalog

```
CREATE TABLE DWS_FACT_DAY_ORG_GCATE_SAL 
(
    BRD_NO                    STRING     ,
    BRD_DTL_NO                STRING     ,
    ORG_LNO                   STRING     ,
    STORE_LNO                 STRING     ,
    M_PRO_CATE_NO             STRING     ,
    M_PRO_GENDER_NO           STRING     ,
    IS_SAT_FLAG               SMALLINT   ,
    STORE_BRD                 STRING     ,
    ORG_NEW_NO                STRING     ,
    M_PRO_CATE_NAME           STRING     ,
    M_PRO_GENDER_NAME         STRING     ,
    SAL_QTY                   BIGINT     ,--使用聚合count 必须声明 BIGINT，此声明与ORACLE类型无关，ORACLE类型可以是number，DECIMAL，vachar等
    SAL_AMT                   DECIMAL(38,4) ,--使用聚合SUM 必须声明DECIMAL(38,4)
    SAL_NOS_PRM_AMT           DECIMAL(38,4) ,
    SAL_PRM_AMT               DECIMAL(38,4) 
) 
  WITH (
    'connector.type' = 'oracle',
    'oracle.primary-key-columns' = 'BRD_NO,BRD_DTL_NO,ORG_LNO,STORE_LNO,IS_SAT_FLAG,STORE_BRD,ORG_NEW_NO,PRO_CATE_NO,PRO_GENDER_NO,PRO_CATE_NAME,PRO_GENDER_NAME',--更新主键，没有侧只能 append
    'oracle.table' = 'DWS_FACT_DAY_ORG_GCATE_SAL'--表名
);
```

Other catalogs
```
CREATE TABLE DWS_FACT_DAY_ORG_GCATE_SAL 
(
    BRD_NO                    STRING     ,
    BRD_DTL_NO                STRING     ,
    ORG_LNO                   STRING     ,
    STORE_LNO                 STRING     ,
    M_PRO_CATE_NO             STRING     ,
    M_PRO_GENDER_NO           STRING     ,
    IS_SAT_FLAG               SMALLINT   ,
    STORE_BRD                 STRING     ,
    ORG_NEW_NO                STRING     ,
    M_PRO_CATE_NAME           STRING     ,
    M_PRO_GENDER_NAME         STRING     ,
    SAL_QTY                   BIGINT     ,--使用聚合count 必须声明 BIGINT，此声明与ORACLE类型无关，ORACLE类型可以是number，DECIMAL，vachar等
    SAL_AMT                   DECIMAL(38,4) ,--使用聚合SUM 必须声明DECIMAL(38,4)
    SAL_NOS_PRM_AMT           DECIMAL(38,4) ,
    SAL_PRM_AMT               DECIMAL(38,4) 
) 
  WITH (
    'connector.type' = 'oracle',
    'oracle.url' = 'jdbc:oracle:thin:@127.0.0.1:1521:dmpdb',--JDBC连接串（后台配置后可忽略）
    'oracle.username' = 'oracle',--用户名（后台配置后可忽略）
    'oracle.password' = 'oracle',--密码（后台配置后可忽略）
    'oracle.primary-key-columns' = 'BRD_NO,BRD_DTL_NO,ORG_LNO,STORE_LNO,IS_SAT_FLAG,STORE_BRD,ORG_NEW_NO,PRO_CATE_NO,PRO_GENDER_NO,PRO_CATE_NAME,PRO_GENDER_NAME',--更新主键，没有侧只能 append
    'oracle.schema' = 'oracle',--属主名
    'oracle.table' = 'DWS_FACT_DAY_ORG_GCATE_SAL'--表名
);
```

Renaming a table:
```
ALTER TABLE TestTable RENAME TO TestTableRen
```

Dropping a table:
```sql
DROP TABLE TestTableRen
```

#### Creating a OracleTable directly with OracleCatalog

The OracleCatalog also exposes a simple `createTable` method that required only the where table configuration,
including schema, partitioning, replication, etc. can be specified using a `OracleTableInfo` object.

Use the `createTableIfNotExists` method, that takes a `ColumnSchemasFactory` and
a `CreateTableOptionsFactory` parameter, that implement respectively `getColumnSchemas()`
returning a list of Oracle [ColumnSchema](https://oracle.apache.org/apidocs/org/apache/oracle/ColumnSchema.html) objects;
 and  `getCreateTableOptions()` returning a
[CreateTableOptions](https://oracle.apache.org/apidocs/org/apache/oracle/client/CreateTableOptions.html) object.

This example shows the creation of a table called `ExampleTable` with two columns,
`first` being a primary key; and configuration of replicas and hash partitioning.

```java
OracleTableInfo tableInfo = OracleTableInfo
    .forTable("ExampleTable")
    .createTableIfNotExists(
        () ->
            Lists.newArrayList(
                new ColumnSchema
                    .ColumnSchemaBuilder("first", Type.INT32)
                    .key(true)
                    .build(),
                new ColumnSchema
                    .ColumnSchemaBuilder("second", Type.STRING)
                    .build()
            ),
        () -> new CreateTableOptions()
            .setNumReplicas(1)
            .addHashPartitions(Lists.newArrayList("first"), 2));

catalog.createTable(tableInfo, false);
```
The example uses lambda expressions to implement the functional interfaces.

Read more about Oracle schema design in the [Oracle docs]().



Note:
* `TIMESTAMP`s are fixed to a precision of 3, and the corresponding Java conversion class is `java.sql.Timestamp` 
* `BINARY` and `VARBINARY` are not yet supported - use `BYTES`, which is a `VARBINARY(2147483647)`
*  `CHAR` and `VARCHAR` are not yet supported - use `STRING`, which is a `VARCHAR(2147483647)`
* `DECIMAL` types are not yet supported

### Known limitations
* Data type limitations (see above).
* SQL Create table: primary keys can only be set by the `oracle.primary-key-columns` property, using the
`PRIMARY KEY` constraint is not yet possible.
* SQL Create table: range partitioning is not supported.
* When getting a table through the Catalog, NOT NULL and PRIMARY KEY constraints are ignored. All columns
are described as being nullable, and not being primary keys.
* Oracle tables cannot be altered through the catalog other than simple renaming

## DataStream API

It is also possible to use the the Oracle connector directly from the DataStream API however we
encourage all users to explore the Table API as it provides a lot of useful tooling when working
with Oracle data.

### Reading tables into a DataStreams

There are 2 main ways of reading a Oracle Table into a DataStream
 1. Using the `OracleCatalog` and the Table API
 2. Using the `OracleRowInputFormat` directly

Using the `OracleCatalog` and Table API is the recommended way of reading tables as it automatically
guarantees type safety and takes care of configuration of our readers.

This is how it works in practice:
```java
StreamTableEnvironment tableEnv = StreamTableEnvironment.create(streamEnv, tableSettings);

tableEnv.registerCatalog("oracle", new OracleCatalog("master:port"));
tableEnv.useCatalog("oracle");

Table table = tableEnv.sqlQuery("SELECT * FROM MyOracleTable");
DataStream<Row> rows = tableEnv.toAppendStream(table, Row.class);
```

The second way of achieving the same thing is by using the `OracleRowInputFormat` directly.
In this case we have to manually provide all information about our table:

```java
OracleTableInfo tableInfo = ...
OracleReaderConfig readerConfig = ...
OracleRowInputFormat inputFormat = new OracleRowInputFormat(readerConfig, tableInfo);

DataStream<Row> rowStream = env.createInput(inputFormat, rowTypeInfo);
```

At the end of the day the `OracleTableSource` is just a convenient wrapper around the `OracleRowInputFormat`.

### Oracle Sink
The connector provides a `OracleSink` class that can be used to consume DataStreams
and write the results into a Oracle table.

The constructor takes 3 or 4 arguments.
 * `OracleWriterConfig` is used to specify the Oracle masters and the flush mode.
 * `OracleTableInfo` identifies the table to be written
 * `OracleOperationMapper` maps the records coming from the DataStream to a list of Oracle operations.
 * `OracleFailureHandler` (optional): If you want to provide your own logic for handling writing failures.

The example below shows the creation of a sink for Row type records of 3 fields. It Upserts each record.
It is assumed that a Oracle table with columns `col1, col2, col3` called `AlreadyExistingTable` exists. Note that if this were not the case,
we could pass a `OracleTableInfo` as described in the [Catalog - Creating a table](#creating-a-table) section,
and the sink would create the table with the provided configuration.

```java

OracleSink<Row> sink = new OracleSink<>(
    writerConfig,
    OracleTableInfo.forTable("AlreadyExistingTable"),
    new RowOperationMapper<>(
            new String[]{"col1", "col2", "col3"},
            AbstractSingleOperationMapper.OracleOperation.UPSERT)
)
```

#### OracleOperationMapper

This section describes the Operation mapping logic in more detail.

The connector supports insert, upsert, update, and delete operations.
The operation to be performed can vary dynamically based on the record.
To allow for more flexibility, it is also possible for one record to trigger
0, 1, or more operations.
For the highest level of control, implement the `OracleOperationMapper` interface.

If one record from the DataStream corresponds to one table operation,
extend the `AbstractSingleOperationMapper` class. An array of column
names must be provided. This must match the Oracle table's schema.

The `getField` method must be overridden, which extracts the value for the table column whose name is
at the `i`th place in the `columnNames` array.
If the operation is one of (`CREATE, UPSERT, UPDATE, DELETE`)
and doesn't depend on the input record (constant during the life of the sink), it can be set in the constructor
of `AbstractSingleOperationMapper`.
It is also possible to implement your own logic by overriding the
`createBaseOperation` method that returns a Oracle [Operation]().

There are pre-defined operation mappers for Pojo, Flink Row, and Flink Tuple types for constant operation, 1-to-1 sinks.
* `PojoOperationMapper`: Each table column must correspond to a POJO field
with the same name. The  `columnNames` array should contain those fields of the POJO that
are present as table columns (the POJO fields can be a superset of table columns).
* `RowOperationMapper` and `TupleOperationMapper`: the mapping is based on position. The
`i`th field of the Row/Tuple corresponds to the column of the table at the `i`th
position in the `columnNames` array.

## Building the connector

The connector can be easily built by using maven:

```

mvn clean install
```

### Running the tests

The integration tests rely on the Oracle test harness which requires the current user to be able to ssh to localhost.

This might not work out of the box on some operating systems (such as Mac OS X).
To solve this problem go to *System Preferences/Sharing* and enable Remote login for your user.
