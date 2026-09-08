/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.trino.datastore;

import org.geotools.api.feature.simple.SimpleFeatureType;
import org.geotools.api.feature.type.AttributeDescriptor;
import org.geotools.api.feature.type.GeometryDescriptor;
import org.geotools.feature.simple.SimpleFeatureTypeBuilder;
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.*;
import java.util.*;

import static org.locationtech.geomesa.trino.datastore.TrinoDataStore.escapeQuotes;

class TrinoSchemaDiscovery {

    private static final Logger LOG = LoggerFactory.getLogger(TrinoSchemaDiscovery.class);

    /** Iceberg table property holding the GeoMesa type name. */
    static final String SFT_NAME_PROPERTY = "geomesa.sft.name";

    /** User-data key on the discovered SimpleFeatureType holding the table's
     *  visibility column name (absent when the table has none). */
    static final String VIS_COLUMN_KEY = "trino.visibility.column";

    /** The per-row visibility column name — {@code __vis__} */
    static final String VIS_COLUMN = "__vis__";

    /** Prefix for Iceberg table properties describing a single column. */
    static final String COLUMN_PREFIX = "geomesa.col.";

    /**
     * Iceberg table property holding the full attribute spec for one column, written for every
     * column. Mirrors {@code IcebergCatalog.columnSpecProperty}, which is what writes it.
     *
     * @param column storage column name
     * @return property key
     */
    static String columnSpecProperty(String column) {
        return COLUMN_PREFIX + column + ".spec";
    }

    private final TrinoDataStore store;

    TrinoSchemaDiscovery(TrinoDataStore store) {
        this.store = store;
    }

    SimpleFeatureType discover(String typeName, String tableName) throws IOException {
        SimpleFeatureTypeBuilder tb = new SimpleFeatureTypeBuilder();
        tb.setName(typeName);
        tb.setNamespaceURI(store.getNamespaceURI());

        String visColumn = null;
        List<AttributeDescriptor> descriptors = new ArrayList<>();
        Map<String, String> sftProps = readSftProperties(tableName);

        try (Connection conn = store.connect()) {
            DatabaseMetaData metaData = conn.getMetaData();
            try (ResultSet rs = metaData.getColumns(store.catalog(), store.trinoSchema(), tableName, null)) {
                while (rs.next()) {
                    String columnName = rs.getString("COLUMN_NAME");
                    if (VIS_COLUMN.equals(columnName)) {
                        visColumn = columnName;
                    } else if (columnName != null && !columnName.startsWith("__")) {
                        // descriptor is a table property
                        String spec = sftProps.get(columnSpecProperty(columnName));
                        if (spec != null) {
                            try {
                                descriptors.add(SimpleFeatureTypes.createDescriptor(spec));
                            } catch (Exception e) {
                                LOG.warn("Error parsing column spec as descriptor: {}", spec, e);
                            }
                        }
                    }
                }
            }
        } catch (SQLException e) {
            throw new IOException("Failed to discover schema for " + typeName, e);
        }

        descriptors.forEach(tb::add);
        descriptors.stream()
                .filter(d -> "true".equals(d.getUserData().get("default")) && d instanceof GeometryDescriptor)
                .findFirst()
                .ifPresent(d -> tb.setDefaultGeometry(d.getLocalName()));

        SimpleFeatureType sft = tb.buildFeatureType();
        if (visColumn != null) {
            sft.getUserData().put(VIS_COLUMN_KEY, visColumn);
        }
        String sftName = sftProps.get(SFT_NAME_PROPERTY);
        if (sftName != null && !sftName.isBlank()) {
            sft.getUserData().put(SFT_NAME_PROPERTY, sftName);
        }
        sftProps.forEach((k, v) -> {
            if (k.startsWith("geomesa.userdata.")) {
                sft.getUserData().put(k.substring("geomesa.userdata.".length()), v);
            }

        });
        return sft;
    }

    /** Reads the {@code geomesa.*} properties - the type name, its user data, and a spec per
     *  column - from the Iceberg {@code <table>$properties} metadata table in one query; empty
     *  when the table carries none or doesn't expose {@code $properties}. Non-fatal, but a table
     *  with no column specs discovers no attributes, there being nothing else to read them from. */
    private Map<String, String> readSftProperties(String tableName) {
        String sql = String.format(
            "SELECT key, value FROM %s.%s.%s",
            escapeQuotes(store.catalog()),
            escapeQuotes(store.trinoSchema()),
            escapeQuotes(tableName + "$properties")
        );
        Map<String, String> props = new HashMap<>();
        try (Connection conn = store.connect();
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {
            while (rs.next()) {
                props.put(rs.getString(1), rs.getString(2));
            }
        } catch (SQLException e) {
            LOG.debug("No readable $properties for '{}' (no SFT metadata): {}",
                tableName, e.getMessage());
        }
        return props;
    }

}
