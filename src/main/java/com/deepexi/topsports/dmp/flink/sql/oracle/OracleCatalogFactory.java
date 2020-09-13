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
import org.apache.flink.annotation.Internal;
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.descriptors.DescriptorProperties;
import org.apache.flink.table.factories.CatalogFactory;
import org.apache.flink.table.factories.TableFactory;
import org.kohsuke.MetaInfServices;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.flink.table.descriptors.CatalogDescriptorValidator.CATALOG_PROPERTY_VERSION;
import static org.apache.flink.table.descriptors.CatalogDescriptorValidator.CATALOG_TYPE;

/**
 * Factory for {@link OracleCatalog}.
 */
@MetaInfServices(TableFactory.class)

@Internal
public class OracleCatalogFactory implements CatalogFactory {

    private static final Logger LOG = LoggerFactory.getLogger(OracleCatalogFactory.class);

    @Override
    public Map<String, String> requiredContext() {
        Map<String, String> context = new HashMap<>();
        context.put(CATALOG_TYPE, OracleTableFactory.ORACLE);
        context.put(CATALOG_PROPERTY_VERSION, "1"); // backwards compatibility
        return context;
    }

    @Override
    public List<String> supportedProperties() {
        List<String> properties = new ArrayList<>();

        properties.add(OracleTableFactory.ORACLE_TABLE);
        properties.add(OracleTableFactory.ORACLE_URL);
        properties.add(OracleTableFactory.ORACLE_DRIVER);
        properties.add(OracleTableFactory.ORACLE_USER_NAME);
        properties.add(OracleTableFactory.ORACLE_PASSWORD);
        properties.add(OracleTableFactory.ORACLE_PRIMARY_KEY_COLS);
        properties.add(OracleTableFactory.ORACLE_SCHEMA);

        return properties;
    }

    @Override
    public Catalog createCatalog(String name, Map<String, String> properties) {
        final DescriptorProperties descriptorProperties = getValidatedProperties(properties);
        //url, String userName, String password, String schema
        return new OracleCatalog(name,new OracleProperties(OracleTableFactory.ORACLE_URL,OracleTableFactory.ORACLE_USER_NAME,OracleTableFactory.ORACLE_PASSWORD,OracleTableFactory.ORACLE_SCHEMA));
    }

    private DescriptorProperties getValidatedProperties(Map<String, String> properties) {
        final DescriptorProperties descriptorProperties = new DescriptorProperties(true);
        descriptorProperties.putProperties(properties);
        descriptorProperties.validateString(OracleTableFactory.ORACLE_URL, false);
        return descriptorProperties;
    }

}
