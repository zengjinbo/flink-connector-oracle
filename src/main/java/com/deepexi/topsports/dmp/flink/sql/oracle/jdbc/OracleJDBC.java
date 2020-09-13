package com.deepexi.topsports.dmp.flink.sql.oracle.jdbc;

import java.sql.Connection;
import java.sql.DriverManager;

/**
 * JDBC连接Oracle
 * @author ouyangjun
 */
public class OracleJDBC {

 
    public static Connection getConnection(String driver, String url, String user, String password) {
        Connection conn = null;
        try {
           // Class.forName(driver); // 加载数据库驱动
            Class.forName(driver);
            conn = DriverManager.getConnection(url, user, password); // 获取数据库连接
           // Properties properties=new Properties();
            //properties.
          //  DriverManager.getConnection(url,)
        } catch (Exception e) {
            System.out.println(e);
        }
        return conn;
    }
}