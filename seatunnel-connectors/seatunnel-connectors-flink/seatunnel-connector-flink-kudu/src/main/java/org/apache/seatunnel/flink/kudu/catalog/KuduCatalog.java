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

package org.apache.seatunnel.flink.kudu.catalog;

import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.catalog.CatalogBaseTable;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.exceptions.TableAlreadyExistException;
import org.apache.kudu.client.KuduClient;
import org.apache.kudu.client.KuduException;
import org.apache.kudu.shaded.com.google.common.collect.Sets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.apache.kudu.shaded.com.google.common.base.Preconditions.checkNotNull;


public class KuduCatalog {
    private static final Logger LOG = LoggerFactory.getLogger(KuduCatalog.class);
    private transient KuduClient kuduClient;

    private final String masters;

    private KuduCatalog(String masters) {
        this.masters = masters;
    }

    public static KuduCatalog of(String masters){
        return new KuduCatalog(masters);
    }

    public void open() {
        this.kuduClient = new KuduClient.KuduClientBuilder(masters).build();
    }

    public void close() {
        try {
            if (kuduClient != null) {
                kuduClient.close();
            }
        } catch (KuduException e) {
            LOG.error("Error while closing kudu client", e);
        }
    }

    public boolean tableExists(String tableName) {
        checkNotNull(tableName);
        try {
            return kuduClient.tableExists(tableName);
        } catch (KuduException e) {
            throw new RuntimeException(e);
        }
    }

    public void createTable(ObjectPath tablePath, CatalogBaseTable table, boolean ignoreIfExists) throws TableAlreadyExistException {
        Map<String, String> tableProperties = table.getOptions();
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

        KuduTableInfo tableInfo = KuduTableUtils.createTableInfo(tableName, tableSchema, tableProperties);

        createTable(tableInfo, ignoreIfExists);
    }

    public void createTable(KuduTableInfo tableInfo, boolean ignoreIfExists) throws CatalogException, TableAlreadyExistException {
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
                KuduException e) {
            throw new CatalogException("Could not create table " + tableInfo.getName(), e);
        }
    }

}
