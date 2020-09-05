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

package com.deepexi.flink.oracle.utils;

import com.deepexi.flink.oracle.connector.ColumnSchema;
import oracle.jdbc.OracleType;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.types.AtomicDataType;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.*;


import java.sql.Timestamp;

import static org.apache.flink.util.Preconditions.checkNotNull;

public class OracleTypeUtils {

    public static DataType toFlinkType(OracleType type, int typeSize, int dataScale) {
        if (type.equals(OracleType.NUMBER)&& dataScale>0)
          return   DataTypes.DECIMAL(typeSize,dataScale);
        else if (type.equals(OracleType.NUMBER)&& typeSize==1)
         return    DataTypes.TINYINT();
        else if (type.equals(OracleType.NUMBER)&& typeSize>1&&typeSize<=4)
          return   DataTypes.SMALLINT();
        else if (type.equals(OracleType.NUMBER)&& typeSize>4&&typeSize<=9)
            return   DataTypes.INT();
        else if (type.equals(OracleType.NUMBER)&& typeSize>9)
            return   DataTypes.BIGINT();
        switch (type) {
            case FLOAT:
                return DataTypes.FLOAT();
            case TIMESTAMP:
                return DataTypes.TIMESTAMP();
            case DATE:
                return DataTypes.DATE();
            default:
                return DataTypes.STRING();

        }
    }

    public static OracleType toOracleType(DataType dataType, ColumnSchema schema) {
        checkNotNull(dataType, "type cannot be null");
 /*       if (type.equals(OracleType.NUMBER)&& dataScale>0)
            return   DataTypes.DECIMAL(typeSize,dataScale);
        else if (type.equals(OracleType.NUMBER)&& typeSize==1)
            return    DataTypes.TINYINT();
        else if (type.equals(OracleType.NUMBER)&& typeSize>1&&typeSize<=4)
            return   DataTypes.SMALLINT();
        else if (type.equals(OracleType.NUMBER)&& typeSize>4&&typeSize<=9)
            return   DataTypes.INT();
        else if (type.equals(OracleType.NUMBER)&& typeSize>9)
            return   DataTypes.BIGINT();
        switch (type) {
            case FLOAT:
                return DataTypes.FLOAT();
            case TIMESTAMP:
                return DataTypes.TIMESTAMP();
            case DATE:
                return DataTypes.DATE();
            default:
                return DataTypes.STRING();*/
return null;
        }
    }



