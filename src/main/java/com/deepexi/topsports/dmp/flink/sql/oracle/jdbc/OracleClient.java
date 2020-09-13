package com.deepexi.topsports.dmp.flink.sql.oracle.jdbc;

import com.deepexi.topsports.dmp.flink.sql.oracle.config.OracleProperties;
import com.deepexi.topsports.dmp.flink.sql.oracle.connector.ColumnSchema;
import com.deepexi.topsports.dmp.flink.sql.oracle.connector.OracleTable;
import com.deepexi.topsports.dmp.flink.sql.oracle.utils.OracleDialect;
import oracle.jdbc.OracleType;
import org.apache.commons.collections.map.CaseInsensitiveMap;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.Serializable;
import java.sql.*;
import java.util.*;
import java.util.regex.Pattern;

/**
 * 数据库JDBC连接工具类
 * Created by yuandl on 2016-12-16.
 */
public class OracleClient implements Serializable {
    private static final Logger LOG = LoggerFactory.getLogger(OracleClient.class);

    private Connection connection = null;
    private OracleProperties properties;
    public OracleClient(OracleProperties properties) throws SQLException {
        this.properties=properties;
        if (connection==null)
        connection = OracleJDBC.getConnection(properties.getDriverClassName(),properties.getUrl(),properties.getUserName(),properties.getPassword());
    }

    /**
     * 执行数据库插入操作
     *
     * @param valueMap  插入数据表中key为列名和value为列对应的值的Map对象
     * @param tableName 要插入的数据库的表名
     * @return 影响的行数
     * @throws SQLException SQL异常
     */
    public  int insert(String tableName, Map<String, Object> valueMap) throws SQLException {

        /**获取数据库插入的Map的键值对的值**/
        Set<String> keySet = valueMap.keySet();
        Iterator<String> iterator = keySet.iterator();
        /**要插入的字段sql，其实就是用key拼起来的**/
        StringBuilder columnSql = new StringBuilder();
        /**要插入的字段值，其实就是？**/
        StringBuilder unknownMarkSql = new StringBuilder();
        Object[] bindArgs = new Object[valueMap.size()];
        int i = 0;
        while (iterator.hasNext()) {
            String key = iterator.next();
            columnSql.append(i == 0 ? "" : ",");
            columnSql.append(key);

            unknownMarkSql.append(i == 0 ? "" : ",");
            unknownMarkSql.append("?");
            bindArgs[i] = valueMap.get(key);
            i++;
        }
        /**开始拼插入的sql语句**/
        StringBuilder sql = new StringBuilder();
        sql.append("INSERT INTO ");
        sql.append(tableName);
        sql.append(" (");
        sql.append(columnSql);
        sql.append(" )  VALUES (");
        sql.append(unknownMarkSql);
        sql.append(" )");
        return executeUpdate(sql.toString(), bindArgs);
    }

    /**
     * 执行数据库插入操作
     *
     * @param datas     插入数据表中key为列名和value为列对应的值的Map对象的List集合
     * @param tableName 要插入的数据库的表名
     * @return 影响的行数
     * @throws SQLException SQL异常
     */
    public  int insertAll(String tableName, List<Map<String, Object>> datas) throws SQLException {
        /**影响的行数**/
        int affectRowCount = -1;
        PreparedStatement preparedStatement = null;
        try {
            /**从数据库连接池中获取数据库连接**/


            Map<String, Object> valueMap = datas.get(0);
            /**获取数据库插入的Map的键值对的值**/
            Set<String> keySet = valueMap.keySet();
            Iterator<String> iterator = keySet.iterator();
            /**要插入的字段sql，其实就是用key拼起来的**/
            StringBuilder columnSql = new StringBuilder();
            /**要插入的字段值，其实就是？**/
            StringBuilder unknownMarkSql = new StringBuilder();
            Object[] keys = new Object[valueMap.size()];
            int i = 0;
            while (iterator.hasNext()) {
                String key = iterator.next();
                keys[i] = key;
                columnSql.append(i == 0 ? "" : ",");
                columnSql.append(key);

                unknownMarkSql.append(i == 0 ? "" : ",");
                unknownMarkSql.append("?");
                i++;
            }
            /**开始拼插入的sql语句**/
            StringBuilder sql = new StringBuilder();
            sql.append("INSERT INTO ");
            sql.append(tableName);
            sql.append(" (");
            sql.append(columnSql);
            sql.append(" )  VALUES (");
            sql.append(unknownMarkSql);
            sql.append(" )");
           // connection.prepareCall()
            /**执行SQL预编译**/
            preparedStatement = connection.prepareStatement(sql.toString());
            /**设置不自动提交，以便于在出现异常的时候数据库回滚**/
            connection.setAutoCommit(false);
        //    System.out.println(sql.toString());
            for (int j = 0; j < datas.size(); j++) {
                for (int k = 0; k < keys.length; k++) {
                    preparedStatement.setObject(k + 1, datas.get(j).get(keys[k]));
                }
                preparedStatement.addBatch();
            }
            int[] arr = preparedStatement.executeBatch();
            connection.commit();
            affectRowCount = arr.length;
          //  System.out.println("成功了插入了" + affectRowCount + "行");
          //  System.out.println();
            LOG.debug("成功了插入了" + affectRowCount + "行");
        } catch (Exception e) {
            if (connection != null) {
                connection.rollback();
            }
            e.printStackTrace();
            throw e;
        } finally {
            if (preparedStatement != null) {
                preparedStatement.close();
            }

        }
        return affectRowCount;
    }
    /**
     * 执行数据库插入操作
     *

     * @return 影响的行数
     * @throws SQLException SQL异常
     */
    public  int mergeInto(String schema, String tableName,  String[] uniqueKeyFields, Map<String, Object> maps) throws SQLException {
      String sql=  OracleDialect.getUpsertStatement(schema,tableName,maps.keySet().toArray(new String[maps.size()]), uniqueKeyFields,true);
        LOG.debug("sql:"+sql);
        return insertBySql(sql.toUpperCase(),maps);
    }

    /**
     * 执行数据库插入操作
     *

     * @return 影响的行数
     * @throws SQLException SQL异常
     */
    public  int insertBySql(String sql, Map<String, Object> maps) throws SQLException {
        /**影响的行数**/
        int affectRowCount = -1;
        PreparedStatement preparedStatement = null;
        try {

            /**执行SQL预编译**/
            preparedStatement = connection.prepareStatement(sql);
            /**设置不自动提交，以便于在出现异常的时候数据库回滚**/
            connection.setAutoCommit(false);
           // System.out.println(sql);
            Iterator<String> it= maps.keySet().iterator();
            int i=1;

            while (it.hasNext())
            {
                String key= it.next();
                Object o=maps.get(key);
                preparedStatement.setObject(i, o);
                if(o!=null)
                LOG.debug(key+":"+o+"（"+o.getClass().getName()+"）,");
                i++;
            }

            preparedStatement.addBatch();
            int[] arr = preparedStatement.executeBatch();
            connection.commit();
            affectRowCount = arr.length;
           // System.out.println("成功了插入了" + affectRowCount + "行");
           // System.out.println();
        } catch (Exception e) {
            if (connection != null) {
                connection.rollback();
            }
            e.printStackTrace();
            throw e;
        } finally {
            if (preparedStatement != null) {
                preparedStatement.close();
            }

        }
        return affectRowCount;
    }
    /**
     * 执行更新操作
     *
     * @param tableName 表名
     * @param valueMap  要更改的值
     * @param whereMap  条件
     * @return 影响的行数
     * @throws SQLException SQL异常
     */
    public  int update(String tableName, Map<String, Object> valueMap, Map<String, Object> whereMap) throws SQLException {
        /**获取数据库插入的Map的键值对的值**/
        Set<String> keySet = valueMap.keySet();
        Iterator<String> iterator = keySet.iterator();
        /**开始拼插入的sql语句**/
        StringBuilder sql = new StringBuilder();
        sql.append("UPDATE ");
        sql.append(tableName);
        sql.append(" SET ");

        /**要更改的的字段sql，其实就是用key拼起来的**/
        StringBuilder columnSql = new StringBuilder();
        int i = 0;
        List<Object> objects = new ArrayList<>();
        while (iterator.hasNext()) {
            String key = iterator.next();
            columnSql.append(i == 0 ? "" : ",");
            columnSql.append(key + " = ? ");
            objects.add(valueMap.get(key));
            i++;
        }
        sql.append(columnSql);

        /**更新的条件:要更改的的字段sql，其实就是用key拼起来的**/
        StringBuilder whereSql = new StringBuilder();
        int j = 0;
        if (whereMap != null && whereMap.size() > 0) {
            whereSql.append(" WHERE ");
            iterator = whereMap.keySet().iterator();
            while (iterator.hasNext()) {
                String key = iterator.next();
                whereSql.append(j == 0 ? "" : " AND ");
                whereSql.append(key + " = ? ");
                objects.add(whereMap.get(key));
                j++;
            }
            sql.append(whereSql);
        }
        return executeUpdate(sql.toString(), objects.toArray());
    }

    /**
     * 执行删除操作
     *
     * @param tableName 要删除的表名
     * @param whereMap  删除的条件
     * @return 影响的行数
     * @throws SQLException SQL执行异常
     */
    public  int delete(String tableName, Map<String, Object> whereMap) throws SQLException {
        /**准备删除的sql语句**/
        StringBuilder sql = new StringBuilder();
        sql.append("DELETE FROM ");
        sql.append(tableName);

        /**更新的条件:要更改的的字段sql，其实就是用key拼起来的**/
        StringBuilder whereSql = new StringBuilder();
        Object[] bindArgs = null;
        if (whereMap != null && whereMap.size() > 0) {
            bindArgs = new Object[whereMap.size()];
            whereSql.append(" WHERE ");
            /**获取数据库插入的Map的键值对的值**/
            Set<String> keySet = whereMap.keySet();
            Iterator<String> iterator = keySet.iterator();
            int i = 0;
            while (iterator.hasNext()) {
                String key = iterator.next();
                whereSql.append(i == 0 ? "" : " AND ");
                whereSql.append(key + " = ? ");
                bindArgs[i] = whereMap.get(key);
                i++;
            }
            sql.append(whereSql);
        }
        return executeUpdate(sql.toString(), bindArgs);
    }

    /**
     * 可以执行新增，修改，删除
     *
     * @param sql      sql语句
     * @param bindArgs 绑定参数
     * @return 影响的行数
     * @throws SQLException SQL异常
     */
    public  int executeUpdate(String sql, Object[] bindArgs) throws SQLException {
        /**影响的行数**/
        int affectRowCount = -1;
        PreparedStatement preparedStatement = null;
        try {
            /**从数据库连接池中获取数据库连接**/
            /**执行SQL预编译**/
            preparedStatement = connection.prepareStatement(sql.toString());
            /**设置不自动提交，以便于在出现异常的时候数据库回滚**/
            connection.setAutoCommit(false);
           // System.out.println(getExecSQL(sql, bindArgs));
            if (bindArgs != null) {
                /**绑定参数设置sql占位符中的值**/
                for (int i = 0; i < bindArgs.length; i++) {
                    preparedStatement.setObject(i + 1, bindArgs[i]);
                }
            }
            /**执行sql**/
            affectRowCount = preparedStatement.executeUpdate();
            connection.commit();
            String operate;
            if (sql.toUpperCase().indexOf("DELETE FROM") != -1) {
                operate = "删除";
            } else if (sql.toUpperCase().indexOf("INSERT INTO") != -1) {
                operate = "新增";
            } else {
                operate = "修改";
            }
           // System.out.println("成功" + operate + "了" + affectRowCount + "行");
         //.   System.out.println();
        } catch (Exception e) {
            if (connection != null) {
                connection.rollback();
            }
            e.printStackTrace();
            throw e;
        } finally {
            if (preparedStatement != null) {
                preparedStatement.close();
            }

        }
        return affectRowCount;
    }

    /**
     * 通过sql查询数据,
     * 慎用，会有sql注入问题
     *
     * @param sql
     * @return 查询的数据集合
     * @throws SQLException
     */
    public  List<Map<String, String>> query(String sql) throws SQLException {
        return executeQuery(sql, null);
    }

    /**
     * 执行sql通过 Map<String, Object>限定查询条件查询
     *
     * @param tableName 表名
     * @param whereMap  where条件
     * @return List<Map<String, Object>>
     * @throws SQLException
     */
    public  List<Map<String, String>> query(String tableName,
                                                  Map<String, Object> whereMap) throws Exception {
        String whereClause = "";
        Object[] whereArgs = null;
        if (whereMap != null && whereMap.size() > 0) {
            Iterator<String> iterator = whereMap.keySet().iterator();
            whereArgs = new Object[whereMap.size()];
            int i = 0;
            while (iterator.hasNext()) {
                String key = iterator.next();
                whereClause += (i == 0 ? "" : " AND ");
                whereClause += (key + " = ? ");
                whereArgs[i] = whereMap.get(key);
                i++;
            }
        }
        return query(tableName, false, null, whereClause, whereArgs, null, null, null, null);
    }

    /**
     * 执行sql条件参数绑定形式的查询
     *
     * @param tableName   表名
     * @param whereClause where条件的sql
     * @param whereArgs   where条件中占位符中的值
     * @return List<Map<String, Object>>
     * @throws SQLException
     */
    public  List<Map<String, String>> query(String tableName,
                                                  String whereClause,
                                                  String[] whereArgs) throws SQLException {
        return query(tableName, false, null, whereClause, whereArgs, null, null, null, null);
    }

    /**
     * 执行全部结构的sql查询
     *
     * @param tableName     表名
     * @param distinct      去重
     * @param columns       要查询的列名
     * @param selection     where条件
     * @param selectionArgs where条件中占位符中的值
     * @param groupBy       分组
     * @param having        筛选
     * @param orderBy       排序
     * @param limit         分页
     * @return List<Map<String, Object>>
     * @throws SQLException
     */
    public  List<Map<String, String>> query(String tableName,
                                                  boolean distinct,
                                                  String[] columns,
                                                  String selection,
                                                  Object[] selectionArgs,
                                                  String groupBy,
                                                  String having,
                                                  String orderBy,
                                                  String limit) throws SQLException {
        String sql = buildQueryString(distinct, tableName, columns, selection, groupBy, having, orderBy, limit);
        return executeQuery(sql, selectionArgs);

    }

    /**
     * 执行查询
     *
     * @param sql      要执行的sql语句
     * @param bindArgs 绑定的参数
     * @return List<Map<String, Object>>结果集对象
     * @throws SQLException SQL执行异常
     */
    public  List<Map<String, String>> executeQuery(String sql, Object[] bindArgs) throws SQLException {
        List<Map<String, String>> datas = new ArrayList<>();
        PreparedStatement preparedStatement = null;
        Statement statement=null;
        getConnection();
        ResultSet resultSet = null;

        try {
            if (bindArgs != null) {
                /**获取数据库连接池中的连接**/
                preparedStatement = connection.prepareStatement(sql);

                /**设置sql占位符中的值**/
                for (int i = 0; i < bindArgs.length; i++) {
                    preparedStatement.setObject(i + 1, bindArgs[i]);
                }
              //  System.out.println(getExecSQL(sql, bindArgs));
                /**执行sql语句，获取结果集**/
                resultSet = preparedStatement.executeQuery();
            } else
            {
                statement=   connection.createStatement();
                resultSet= statement.executeQuery(sql);

            }
            datas = getDatas(resultSet);
        } catch (Exception e) {
            e.printStackTrace();
            throw e;
        } finally {
            if (resultSet != null) {
                resultSet.close();
            }
            if (preparedStatement != null&&!preparedStatement.isClosed()) {
                preparedStatement.close();
            }
 /*           if (connection != null) {
                connection.close();
            }*/

        }
        return datas;
    }


    /**
     * 将结果集对象封装成List<Map<String, Object>> 对象
     *
     * @param resultSet 结果多想
     * @return 结果的封装
     * @throws SQLException
     */
    private  List<Map<String, String>> getDatas(ResultSet resultSet) throws SQLException {
        List<Map<String, String>> datas = new ArrayList<>();
        /**获取结果集的数据结构对象**/
        ResultSetMetaData metaData = resultSet.getMetaData();
        while (resultSet.next()) {
            Map<String, String> rowMap = new CaseInsensitiveMap();
            for (int i = 1; i <= metaData.getColumnCount(); i++) {
                rowMap.put(metaData.getColumnName(i), resultSet.getString(i));
            }
            datas.add(rowMap);
        }

        return datas;
    }


    /**
     * Build an SQL query string from the given clauses.
     *
     * @param distinct true if you want each row to be unique, false otherwise.
     * @param tables   The table names to compile the query against.
     * @param columns  A list of which columns to return. Passing null will
     *                 return all columns, which is discouraged to prevent reading
     *                 data from storage that isn't going to be used.
     * @param where    A filter declaring which rows to return, formatted as an SQL
     *                 WHERE clause (excluding the WHERE itself). Passing null will
     *                 return all rows for the given URL.
     * @param groupBy  A filter declaring how to group rows, formatted as an SQL
     *                 GROUP BY clause (excluding the GROUP BY itself). Passing null
     *                 will cause the rows to not be grouped.
     * @param having   A filter declare which row groups to include in the cursor,
     *                 if row grouping is being used, formatted as an SQL HAVING
     *                 clause (excluding the HAVING itself). Passing null will cause
     *                 all row groups to be included, and is required when row
     *                 grouping is not being used.
     * @param orderBy  How to order the rows, formatted as an SQL ORDER BY clause
     *                 (excluding the ORDER BY itself). Passing null will use the
     *                 default sort order, which may be unordered.
     * @param limit    Limits the number of rows returned by the query,
     *                 formatted as LIMIT clause. Passing null denotes no LIMIT clause.
     * @return the SQL query string
     */
    private  String buildQueryString(
            boolean distinct, String tables, String[] columns, String where,
            String groupBy, String having, String orderBy, String limit) {
        if (isEmpty(groupBy) && !isEmpty(having)) {
            throw new IllegalArgumentException(
                    "HAVING clauses are only permitted when using a groupBy clause");
        }
        if (!isEmpty(limit) && !sLimitPattern.matcher(limit).matches()) {
            throw new IllegalArgumentException("invalid LIMIT clauses:" + limit);
        }

        StringBuilder query = new StringBuilder(120);

        query.append("SELECT ");
        if (distinct) {
            query.append("DISTINCT ");
        }
        if (columns != null && columns.length != 0) {
            appendColumns(query, columns);
        } else {
            query.append(" * ");
        }
        query.append("FROM ");
        query.append(tables);
        appendClause(query, " WHERE ", where);
        appendClause(query, " GROUP BY ", groupBy);
        appendClause(query, " HAVING ", having);
        appendClause(query, " ORDER BY ", orderBy);
        appendClause(query, " LIMIT ", limit);
        return query.toString();
    }

    /**
     * Add the names that are non-null in columns to s, separating
     * them with commas.
     */
    private  void appendColumns(StringBuilder s, String[] columns) {
        int n = columns.length;

        for (int i = 0; i < n; i++) {
            String column = columns[i];

            if (column != null) {
                if (i > 0) {
                    s.append(", ");
                }
                s.append(column);
            }
        }
        s.append(' ');
    }

    /**
     * addClause
     *
     * @param s      the add StringBuilder
     * @param name   clauseName
     * @param clause clauseSelection
     */
    private  void appendClause(StringBuilder s, String name, String clause) {
        if (!isEmpty(clause)) {
            s.append(name);
            s.append(clause);
        }
    }

    /**
     * Returns true if the string is null or 0-length.
     *
     * @param str the string to be examined
     * @return true if str is null or zero length
     */
    private  boolean isEmpty(@Nullable CharSequence str) {
        if (str == null || str.length() == 0)
            return true;
        else
            return false;
    }

    /**
     * the pattern of limit
     */
    private  final Pattern sLimitPattern =
            Pattern.compile("\\s*\\d+\\s*(,\\s*\\d+\\s*)?");

    /**
     * After the execution of the complete SQL statement, not necessarily the actual implementation of the SQL statement
     *
     * @param sql      SQL statement
     * @param bindArgs Binding parameters
     * @return Replace? SQL statement executed after the
     */
    private  String getExecSQL(String sql, Object[] bindArgs) {
        StringBuilder sb = new StringBuilder(sql);
        if (bindArgs != null && bindArgs.length > 0) {
            int index = 0;
            for (int i = 0; i < bindArgs.length; i++) {
                index = sb.indexOf("?", index);
                sb.replace(index, index + 1, String.valueOf(bindArgs[i]));
            }
        }
        return sb.toString();
    }

    public void getConnection() {
        if (connection==null)
        connection = OracleJDBC.getConnection(properties.getDriverClassName(),properties.getUrl(),properties.getUserName(),properties.getPassword());

    }

    public  boolean tableExists(String owner, String tableName) throws SQLException {
        getConnection();
       String sql="select  count(1) cnt from all_tables t  where t.OWNER='" +owner.toUpperCase()+"' and t.TABLE_NAME ='" +tableName.toUpperCase()+"'";
        List<Map<String, String>> cnt= this.executeQuery(sql,null);
        if (cnt!=null&&cnt.size()==1&&Integer.parseInt(String.valueOf(cnt.get(0).get("CNT")))==1)
            return true;
        else
            return false;


    }

    public OracleTable getTable( String owner,String tableName) throws SQLException {
        String sql = "SELECT T.OWNER,\n" +
                "       T.TABLE_NAME,\n" +
                "       C.COLUMN_NAME,\n" +
                "       C.DATA_SCALE AS DATA_SCALE, --字段精度\n" +
                "       C.DATA_LENGTH,"+
                "       C.DATA_TYPE\n" +
                "  FROM ALL_TAB_COMMENTS T\n" +
                "  LEFT JOIN ALL_TAB_COLUMNS C\n" +
                "    ON T.TABLE_NAME = C.TABLE_NAME\n" +
                "   AND T.OWNER = C.OWNER\n" +
                " WHERE T.OWNER = '" +owner.toUpperCase()+"'\n"+
                "   AND T.TABLE_NAME = '" +tableName.toUpperCase()+"'\n"+
                " ORDER BY T. TABLE_NAME, C.COLUMN_NAME\n";
        List<Map<String, String>> list= this.executeQuery(sql,null);
        List<ColumnSchema> columnSchemas =new ArrayList<ColumnSchema>();
        if (list.size()<1)
       return null;
        for (Map<String, String> map:list
             ) {
            String str=map.get("DATA_SCALE");
            int dataScale = StringUtils.isEmpty(str)?0:Integer.parseInt(str);
            String strLength=map.get("DATA_LENGTH");
            int dataLength= StringUtils.isEmpty(strLength)?0:Integer.parseInt(strLength);;
            columnSchemas.add(new ColumnSchema(map.get("COLUMN_NAME").toString(),getOracleType(map.get("DATA_TYPE").toString()),dataLength,dataScale));
        }
       return new OracleTable(columnSchemas,tableName,owner);
    }
    public OracleTable getTable(String tableName) throws SQLException {
      return   this.getTable(this.properties.getSchema(),tableName);
    }
    public  boolean tableExists(String tableName) throws SQLException {
    return     this.tableExists(this.properties.getSchema(),tableName);
    }
    private OracleType getOracleType(String type)
    {
        return  OracleType.valueOf(type);
    }
    public synchronized void  close()
    {
        if(connection!=null) {
            try {
                connection.close();
            } catch (SQLException throwables) {
                throwables.printStackTrace();
            }
        }
    }
}
