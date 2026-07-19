/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.trino.datastore;

import org.geotools.api.feature.type.AttributeDescriptor;
import org.geotools.feature.AttributeTypeBuilder;
import org.geotools.referencing.crs.DefaultGeographicCRS;
import org.locationtech.jts.geom.Geometry;

import java.sql.Types;
import java.util.Date;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class TrinoTypeMapper {

    private static final Logger LOG = LoggerFactory.getLogger(TrinoTypeMapper.class);

    /**
     * Columns hidden from the GeoTools schema: the spatial extension's
     * double-underscore-bracketed bookkeeping columns. This covers {@code __fid__},
     * {@code __vis__}, and every geometry companion ({@code __<X>_bbox__},
     * {@code __<X>_z2__}, {@code __<X>_xz2__}) for any geom column name — not just
     * the legacy {@code geom}. The geometry base columns themselves (e.g. {@code
     * geom}, {@code center}) are not bracketed and remain visible.
     */
    static boolean isHidden(String columnName) {
        return columnName.startsWith("__") && columnName.endsWith("__");
    }

    static AttributeDescriptor toDescriptor(String name, int sqlType,
                                             boolean isGeometry, Class<?> geometryBinding, int srid) {
        AttributeTypeBuilder b = new AttributeTypeBuilder();
        b.setName(name);
        b.setNillable(true);

        if (isGeometry) {
            org.geotools.api.referencing.crs.CoordinateReferenceSystem crs =
                DefaultGeographicCRS.WGS84;
            if (srid > 0 && srid != 4326) {
                try {
                    crs = org.geotools.referencing.CRS.decode("EPSG:" + srid, true);
                } catch (Exception e) {
                    // fall back to WGS84
                }
            }
            b.setCRS(crs);
            Class<?> binding = geometryBinding != null && Geometry.class.isAssignableFrom(geometryBinding)
                ? geometryBinding : Geometry.class;
            b.setBinding(binding);
            return b.buildDescriptor(name, b.buildGeometryType());
        }

        Class<?> binding = switch (sqlType) {
            case Types.VARCHAR, Types.LONGNVARCHAR, Types.NVARCHAR -> String.class;
            case Types.BIGINT                                       -> Long.class;
            case Types.INTEGER, Types.SMALLINT, Types.TINYINT      -> Integer.class;
            case Types.DOUBLE, Types.FLOAT, Types.REAL             -> Double.class;
            case Types.BOOLEAN, Types.BIT                          -> Boolean.class;
            case Types.TIMESTAMP, Types.TIMESTAMP_WITH_TIMEZONE    -> Date.class;
            case Types.BINARY, Types.VARBINARY, Types.LONGVARBINARY -> byte[].class;
            // Truly unmapped SQL type (e.g. DECIMAL, ARRAY, ROW): fall back to opaque bytes,
            // but warn so the unhandled column type is visible rather than silently lossy.
            default                                                 -> unmappedBinding(sqlType, name);
        };

        b.setBinding(binding);
        return b.buildDescriptor(name);
    }

    /** Fallback binding for an SQL type with no explicit mapping: opaque bytes, logged once
     *  per occurrence at WARNING so an unhandled column type (DECIMAL, ARRAY, ROW, …) surfaces
     *  instead of silently becoming a byte[]. */
    private static Class<?> unmappedBinding(int sqlType, String name) {
        LOG.warn("No GeoTools binding for SQL type " + sqlType + " on column '" + name
            + "'; exposing it as byte[] (opaque).");
        return byte[].class;
    }
}
