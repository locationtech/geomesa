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
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.Point;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.*;
import java.util.*;

import static org.locationtech.geomesa.trino.datastore.TrinoDataStore.escapeQuotes;

class TrinoSchemaDiscovery {

    private static final Logger LOG = LoggerFactory.getLogger(TrinoSchemaDiscovery.class);

    /** Iceberg table property holding the GeoMesa-encoded SimpleFeatureType spec. */
    static final String SFT_SPEC_PROPERTY = "geomesa.sft.spec";

    /** Iceberg table property holding the GeoMesa type name. */
    static final String SFT_NAME_PROPERTY = "geomesa.sft.name";

    private final TrinoDataStore store;

    TrinoSchemaDiscovery(TrinoDataStore store) {
        this.store = store;
    }

    SimpleFeatureType discover(String typeName) throws IOException {
        SimpleFeatureTypeBuilder tb = new SimpleFeatureTypeBuilder();
        tb.setName(typeName);
        tb.setNamespaceURI(store.getNamespaceURI());

        String sql = String.format(
            "SELECT * FROM %s.%s.%s LIMIT 0",
            escapeQuotes(store.catalog()),
            escapeQuotes(store.trinoSchema()),
            escapeQuotes(typeName)
        );

        Map<String, String> sftProps = readSftProperties(typeName);
        String sftSpec = sftProps.get(SFT_SPEC_PROPERTY);
        Map<String, Class<?>> sftBindings = (sftSpec == null || sftSpec.isBlank())
            ? Map.of() : geometryBindingsFromSpec(typeName, sftSpec);

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
                if (name.equals(visColumn)) continue;  // vis column is metadata, not an SFT attribute

                boolean isGeom = geometryColumnNames.contains(name);
                Class<?> geomBinding = isGeom ? resolveGeometryBinding(name, allNames, sftBindings) : null;
                // SRID is no longer carried in a table property; default to WGS84.
                int srid = isGeom ? 4326 : 0;
                var descriptor =
                    TrinoTypeMapper.toDescriptor(name, meta.getColumnType(i), isGeom, geomBinding, srid);
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
        String sftName = sftProps.get(SFT_NAME_PROPERTY);
        if (sftName != null && !sftName.isBlank()) {
            sft.getUserData().put(SFT_NAME_PROPERTY, sftName);
        }
        return sft;
    }

    /** The JTS geometry binding for a geometry column: the subtype declared by the stored SFT
     *  when it names this attribute, else {@link Point} for a {@code __<name>_z2__} companion,
     *  generic {@link Geometry} otherwise. */
    static Class<?> resolveGeometryBinding(String name, Set<String> allNames,
                                           Map<String, Class<?>> sftBindings) {
        Class<?> fromSft = sftBindings.get(name);
        if (fromSft != null) {
            return fromSft;
        }
        return isPointColumn(name, allNames) ? Point.class : Geometry.class;
    }

    /** Parses a GeoMesa SFT spec into geometry-attribute-name → JTS subtype. */
    static Map<String, Class<?>> geometryBindingsFromSpec(String typeName, String spec) {
        try {
            SimpleFeatureType sft = SimpleFeatureTypes.createType(typeName, spec);
            Map<String, Class<?>> bindings = new LinkedHashMap<>();
            for (AttributeDescriptor d : sft.getAttributeDescriptors()) {
                if (d instanceof GeometryDescriptor) {
                    bindings.put(d.getLocalName(), d.getType().getBinding());
                }
            }
            return bindings;
        } catch (RuntimeException e) {
            LOG.warn("Could not parse stored GeoMesa SFT for '{}' ({}='{}'): {}; "
                + "falling back to heuristic geometry binding",
                typeName, SFT_SPEC_PROPERTY, spec, e.getMessage());
            return Map.of();
        }
    }

    /** Reads the {@code geomesa.sft.*} properties (spec + name) from the Iceberg
     *  {@code <table>$properties} metadata table in one query; empty when the table carries none
     *  or doesn't expose {@code $properties}. Non-fatal: any failure just disables SFT-driven
     *  binding/name for this table. */
    private Map<String, String> readSftProperties(String typeName) {
        String sql = String.format(
            "SELECT key, value FROM %s.%s.%s WHERE key IN ('%s', '%s')",
            escapeQuotes(store.catalog()),
            escapeQuotes(store.trinoSchema()),
            escapeQuotes(typeName + "$properties"),
            SFT_SPEC_PROPERTY, SFT_NAME_PROPERTY
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
                typeName, e.getMessage());
        }
        return props;
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
