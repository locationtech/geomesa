/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.trino.spatial.iceberg;

import org.apache.iceberg.types.Type;

/**
 * Maps between Iceberg types and Trino type names for the spatial extension.
 *
 * Trino's iceberg connector exposes `TypeConverter` only as static methods,
 * which cannot be subclassed or stubbed. This class exists as an
 * instance-method alternative so the mapping logic can be unit-tested in
 * isolation (see GeometryTypeConverterTest). It is intentionally NOT
 * wired into any live path: the connector exposes {@code geom} as plain
 * VARBINARY and {@code SpatialConnectorMetadata} delegates {@code
 * getTableMetadata} / {@code getColumnMetadata} unchanged (the earlier
 * VARBINARY → Geometry overlay was removed: it needed a wrapping page source
 * materializing every row plus virtual WKB columns/UDFs to avoid that cost on
 * filter paths, and benchmarked at parity with an explicit
 * {@code ST_GeomFromBinary(geom)} wrap — so it cost ~500 LOC and a class of
 * classloader-bridging failures for a cosmetic gain). Retained for future use,
 * e.g. a writer-side mapping or an Iceberg-side `GEOMETRY` TypeId migration.
 */
public class GeometryTypeConverter {

    private static final String TRINO_GEOMETRY_TYPE_NAME = "Geometry";

    /** Creates a converter. */
    public GeometryTypeConverter() {}

    /**
     * Returns the Trino type name for a given Iceberg type.
     *
     * @param icebergType iceberg type to map
     * @return trino type name
     */
    public String toTrinoTypeName(Type icebergType) {
        if (icebergType instanceof GeometryType) {
            return TRINO_GEOMETRY_TYPE_NAME;
        }
        return defaultMapping(icebergType);
    }

    /**
     * Returns true if the Trino type name represents a geometry column.
     *
     * @param trinoTypeName trino type name to test
     * @return true if the name represents a geometry column
     */
    public boolean isGeometryType(String trinoTypeName) {
        return TRINO_GEOMETRY_TYPE_NAME.equalsIgnoreCase(trinoTypeName);
    }

    private String defaultMapping(Type type) {
        return switch (type.typeId()) {
            case BOOLEAN -> "boolean";
            case INTEGER -> "integer";
            case LONG -> "bigint";
            case FLOAT -> "real";
            case DOUBLE -> "double";
            case DATE -> "date";
            case TIME -> "time(6)";
            case TIMESTAMP -> {
                var ts = (org.apache.iceberg.types.Types.TimestampType) type;
                yield ts.shouldAdjustToUTC() ? "timestamp(6) with time zone" : "timestamp(6)";
            }
            case STRING -> "varchar";
            case UUID -> "uuid";
            case FIXED -> "varbinary";
            case BINARY -> "varbinary";
            case DECIMAL -> {
                var dt = (org.apache.iceberg.types.Types.DecimalType) type;
                yield "decimal(" + dt.precision() + "," + dt.scale() + ")";
            }
            case STRUCT, LIST, MAP -> throw new UnsupportedOperationException("Complex Iceberg types not yet supported: " + type);
            default -> throw new UnsupportedOperationException("Unsupported Iceberg type: " + type);
        };
    }
}
