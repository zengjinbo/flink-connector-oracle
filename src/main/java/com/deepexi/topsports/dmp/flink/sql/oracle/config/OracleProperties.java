package com.deepexi.topsports.dmp.flink.sql.oracle.config;

import java.io.Serializable;

/**
 * @author 曾进波
 * @ClassName: OracleProperties
 * @Description: TODO(一句话描述这个类)
 * @date
 * @Copyright ? 北京滴普科技有限公司
 */
public class OracleProperties implements Serializable {
    private String url;
    private String userName;
    private String password;
    private String tableName;
    private String schema;
    private String driverClassName="oracle.jdbc.driver.OracleDriver";
    public OracleProperties(String url, String userName, String password, String tableName, String schema) {
        this.url = url;
        this.userName = userName;
        this.password = password;
        this.tableName = tableName;
        this.schema = schema;
    }

    public OracleProperties(String url, String userName, String password, String tableName, String schema, String driverClassName) {
        this.url = url;
        this.userName = userName;
        this.password = password;
        this.tableName = tableName;
        this.schema = schema;
        this.driverClassName = driverClassName;
    }


    public OracleProperties(String url, String userName, String password) {
        this.url = url;
        this.userName = userName;
        this.password = password;
        this.schema=userName;
    }

    public OracleProperties(String url, String userName, String password, String schema) {
        this.url = url;
        this.userName = userName;
        this.password = password;
        this.schema = schema;
    }

    public OracleProperties() {
    }

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public String getUserName() {
        return userName;
    }

    public String getDriverClassName() {
        return driverClassName;
    }

    public void setDriverClassName(String driverClassName) {
        this.driverClassName = driverClassName;
    }

    public void setUserName(String userName) {
        this.userName = userName;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public String getSchema() {
        return schema;
    }

    public void setSchema(String schema) {
        this.schema = schema;
    }
}
