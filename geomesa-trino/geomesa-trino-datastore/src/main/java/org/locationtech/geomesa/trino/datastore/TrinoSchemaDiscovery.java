/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.trino.datastore;

import org.geotools.api.feature.simple.SimpleFeatureType;
import org.geotools.feature.simple.SimpleFeatureTypeBuilder;

import java.io.IOException;
import java.sql.*;
import java.util.*;

class TrinoSchemaDiscovery {

    private final TrinoDataStore store;

    TrinoSchemaDiscovery(TrinoDataStore store) {
        this.store = store;
    }

    SimpleFeatureType discover(String typeName) throws IOException {
        SimpleFeatureTypeBuilder tb = new SimpleFeatureTypeBuilder();
        tb.setName(typeName);
        tb.setNamespaceURI(store.getNamespaceURI());

        String sql = String.format("SELECT * FROM \"%s\".\"%s\".\"%s\" LIMIT 0",
            store.catalog(), store.trinoSchema(), typeName);

        String visColumn = null;
        try (Connection conn = store.connect();
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {

            ResultSetMetaData meta = rs.getMetaData();

            // First pass: collect all column names so we can detect geometry columns
            // via the naming convention used by the spatial_iceberg connector — a
            // VARBINARY column X is a geometry column iff at least one of
            // __X_bbox__/__X_z2__/__X_xz2__ exists. Same rule the connector uses; no
            // table-property dependency.
            Set<String> allNames = new HashSet<>();
            for (int i = 1; i <= meta.getColumnCount(); i++) {
                allNames.add(meta.getColumnName(i));
            }
            Set<String> geometryColumnNames = discoverGeometryColumnNames(allNames);
            visColumn = discoverVisibilityColumn(allNames);

            boolean defaultGeomSet = false;
            for (int i = 1; i <= meta.getColumnCount(); i++) {
                String name = meta.getColumnName(i);
                if (TrinoTypeMapper.isHidden(name)) continue;
                if (name.equals(visColumn)) continue;  // vis column is metadata, not a SFT attribute

                boolean isGeom = geometryColumnNames.contains(name);
                // A __X_z2__ companion marks a point-only geometry column, so the
                // attribute can be bound to Point rather than the generic Geometry.
                boolean isPoint = isGeom && isPointColumn(name, allNames);
                // SRID is no longer carried in a table property; default to WGS84.
                int srid = isGeom ? 4326 : 0;
                var descriptor =
                    TrinoTypeMapper.toDescriptor(name, meta.getColumnType(i), isGeom, isPoint, srid);
                tb.add(descriptor);

                if (isGeom && !defaultGeomSet) {
                    tb.setDefaultGeometry(name);
                    defaultGeomSet = true;
                }
            }
        } catch (SQLException e) {
            throw new IOException("Failed to discover schema for " + typeName, e);
        }

        SimpleFeatureType sft = tb.buildFeatureType();
        if (visColumn != null) {
            sft.getUserData().put(VIS_COLUMN_KEY, visColumn);
        }
        return sft;
    }

    /** Returns the set of column names that are geometry columns under the
     *  naming convention: a base name {@code X} is a geometry column iff at least
     *  one of {@code __X_bbox__}, {@code __X_z2__}, {@code __X_xz2__} appears in
     *  the table's column list. Companions themselves (names starting and ending
     *  with {@code __}) are skipped. */
    static Set<String> discoverGeometryColumnNames(Set<String> allNames) {
        Set<String> result = new LinkedHashSet<>();
        for (String name : allNames) {
            if (name.startsWith("__") && name.endsWith("__")) continue;
            if (allNames.contains("__" + name + "_bbox__")
                    || allNames.contains("__" + name + "_z2__")
                    || allNames.contains("__" + name + "_xz2__")) {
                result.add(name);
            }
        }
        return result;
    }

    /** True when the geometry column carries a {@code __<name>_z2__} companion —
     *  point-only by the spatial-column convention (non-point data uses XZ2). */
    static boolean isPointColumn(String name, Set<String> allNames) {
        return allNames.contains("__" + name + "_z2__");
    }

    /** User-data key on the discovered SimpleFeatureType holding the table's
     *  visibility column name (absent when the table has none). */
    static final String VIS_COLUMN_KEY = "trino.visibility.column";

    /** The per-row visibility column name — {@code __vis__} */
    static final String VIS_COLUMN = "__vis__";

    /** Returns the table's visibility column name ({@code __vis__}), or null if
     *  the table has none. */
    static String discoverVisibilityColumn(Set<String> allNames) {
        return allNames.contains(VIS_COLUMN) ? VIS_COLUMN : null;
    }

}
