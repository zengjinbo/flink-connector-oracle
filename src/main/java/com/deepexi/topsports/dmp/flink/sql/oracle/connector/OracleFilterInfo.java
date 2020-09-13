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
package com.deepexi.topsports.dmp.flink.sql.oracle.connector;

import oracle.jdbc.rowset.OraclePredicate;
import org.apache.commons.math3.ode.events.FilterType;
import org.apache.flink.annotation.PublicEvolving;


import java.io.Serializable;
import java.util.List;

@PublicEvolving
public class OracleFilterInfo implements Serializable {

    private String column;
    private FilterType type;
    private Object value;

    private OracleFilterInfo() { }

 /*   public OraclePredicate toPredicate(Schema schema) {
        return toPredicate(schema.getColumn(this.column));
    }

    public OraclePredicate toPredicate(ColumnSchema column) {
        OraclePredicate predicate;
        switch (this.type) {
            case IS_IN:
                predicate = OraclePredicate.newInListPredicate(column, (List<?>) this.value);
                break;
            case IS_NULL:
                predicate = OraclePredicate.newIsNullPredicate(column);
                break;
            case IS_NOT_NULL:
                predicate = OraclePredicate.newIsNotNullPredicate(column);
                break;
            default:
                predicate = predicateComparator(column);
                break;
        }
        return predicate;
    }*/
/*
    private OraclePredicate predicateComparator(ColumnSchema column) {

        OraclePredicate.ComparisonOp comparison = this.type.comparator;

        OraclePredicate predicate;

        switch (column.getType()) {
            case STRING:
                predicate = OraclePredicate.newComparisonPredicate(column, comparison, (String) this.value);
                break;
            case FLOAT:
                predicate = OraclePredicate.newComparisonPredicate(column, comparison, (float) this.value);
                break;
            case INT8:
                predicate = OraclePredicate.newComparisonPredicate(column, comparison, (byte) this.value);
                break;
            case INT16:
                predicate = OraclePredicate.newComparisonPredicate(column, comparison, (short) this.value);
                break;
            case INT32:
                predicate = OraclePredicate.newComparisonPredicate(column, comparison, (int) this.value);
                break;
            case INT64:
                predicate = OraclePredicate.newComparisonPredicate(column, comparison, (long) this.value);
                break;
            case DOUBLE:
                predicate = OraclePredicate.newComparisonPredicate(column, comparison, (double) this.value);
                break;
            case BOOL:
                predicate = OraclePredicate.newComparisonPredicate(column, comparison, (boolean) this.value);
                break;
            case UNIXTIME_MICROS:
                Long time = (Long) this.value;
                predicate = OraclePredicate.newComparisonPredicate(column, comparison, time * 1000);
                break;
            case BINARY:
                predicate = OraclePredicate.newComparisonPredicate(column, comparison, (byte[]) this.value);
                break;
            default:
                throw new IllegalArgumentException("Illegal var type: " + column.getType());
        }
        return predicate;
    }
*/
    public enum FilterType {
  /*      GREATER(OraclePredicate.ComparisonOp.GREATER),
        GREATER_EQUAL(OraclePredicate.ComparisonOp.GREATER_EQUAL),
        EQUAL(OraclePredicate.ComparisonOp.EQUAL),
        LESS(OraclePredicate.ComparisonOp.LESS),
        LESS_EQUAL(OraclePredicate.ComparisonOp.LESS_EQUAL),*/
        IS_NOT_NULL(null),
        IS_NULL(null),
        IS_IN(null);


     FilterType(Object o) {
     }
 }

    public static class Builder {
        private OracleFilterInfo filter;

        private Builder(String column) {
            this.filter = new OracleFilterInfo();
            this.filter.column = column;
        }

        public static Builder create(String column) {
            return new Builder(column);
        }

//        public Builder greaterThan(Object value) {
//            return filter(FilterType.GREATER, value);
//        }
//
//        public Builder lessThan(Object value) {
//            return filter(FilterType.LESS, value);
//        }
//
//        public Builder equalTo(Object value) {
//            return filter(FilterType.EQUAL, value);
//        }
//
//        public Builder greaterOrEqualTo(Object value) {
//            return filter(FilterType.GREATER_EQUAL, value);
//        }
//
//        public Builder lessOrEqualTo(Object value) {
//            return filter(FilterType.LESS_EQUAL, value);
//        }
//
        public Builder isNotNull() {
            return filter(FilterType.IS_NOT_NULL, null);
        }

        public Builder isNull() {
            return filter(FilterType.IS_NULL, null);
        }

        public Builder isIn(List<?> values) {
            return filter(FilterType.IS_IN, values);
        }

        public Builder filter(FilterType type, Object value) {
            this.filter.type = type;
            this.filter.value = value;
            return this;
        }

        public OracleFilterInfo build() {
            return filter;
        }
    }

}
