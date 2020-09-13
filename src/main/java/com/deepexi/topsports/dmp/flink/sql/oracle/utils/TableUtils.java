package com.deepexi.topsports.dmp.flink.sql.oracle.utils;


import org.apache.commons.collections.map.CaseInsensitiveMap;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.util.PropertiesUtil;
import org.apache.log4j.Logger;
import scala.util.parsing.json.JSONObject;

import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class TableUtils {
    private final static Logger logger = Logger.getLogger(TableUtils.class);



    public static String getTableFullPath(String schema, String tableName) {
        String[] tableInfoSplit = StringUtils.split(tableName, ".");
        //表明表信息带了schema
        if(tableInfoSplit.length == 2){
            schema = tableInfoSplit[0];
            tableName = tableInfoSplit[1];
        }

        //清理首个字符" 和最后字符 "
        schema = rmStrQuote(schema);
        tableName = rmStrQuote(tableName);

        if (StringUtils.isEmpty(schema)){
            return addQuoteForStr(tableName);
        }

        String schemaAndTabName = schema + "." + tableName;
        return schemaAndTabName;
    }
    public static String addQuoteForStr(String column) {
        return getStartQuote() + column + getEndQuote();
    }
    public static String getStartQuote() {
        return "\"";
    }
    public static String getEndQuote() {
        return "\"";
    }
    /**
     * 清理首个字符" 和最后字符 "
     */
    public static String rmStrQuote(String str){
        if(StringUtils.isEmpty(str)){
            return str;
        }

        if(str.startsWith("\"")){
            str = str.substring(1);
        }

        if(str.endsWith("\"")){
            str = str.substring(0, str.length()-1);
        }

        return str;
    }
    public  static Map<String,String> getTables(List<String> list)
    {
        Map<String,String> map = new CaseInsensitiveMap();
        for (String value:list
        ) {
            map.put(value,value) ;
        }
        return map;
    }



}
