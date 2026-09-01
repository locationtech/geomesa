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
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class TrinoTypeMapper {

    private static final Logger LOG = LoggerFactory.getLogger(TrinoTypeMapper.class);

    /** GeoMesa attribute-option keys. Mirrors {@code SimpleFeatureTypes.AttributeOptions}:
     *  {@code List} attribute declares its element type under {@code subtype},
     *  {@code Map} declares its key and value types under {@code keyclass}/{@code valueclass},
     *  {@code String} holding JSON sets {@code json=true}, and declares its shape under
     *  {@code json-schema} when one can be derived from the Trino type signature
     */
    static final String OPT_SUBTYPE     = "subtype";
    static final String OPT_KEY_CLASS   = "keyclass";
    static final String OPT_VALUE_CLASS = "valueclass";
    static final String OPT_JSON        = "json";
    static final String OPT_JSON_SCHEMA = "json-schema";

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

    /**
     * Back-compatible overload for callers with no parameterized type name.
     */
    static AttributeDescriptor toDescriptor(String name, int sqlType,
                                            boolean isGeometry, Class<?> geometryBinding, int srid) {
        return toDescriptor(name, sqlType, null, isGeometry, geometryBinding, srid);
    }

    /**
     * Maps one Trino column onto a GeoTools attribute descriptor.
     *
     * <p>Scalars map directly.
     * Structural types — {@code array}, {@code map}, {@code row} use {@code typeName}.
     * Given typeName:
     *
     * <ul>
     *   <li>{@code array(<scalar>)} becomes a {@link List} with {@code subtype};</li>
     *   <li>{@code map(<scalar>,<scalar>)} becomes a {@link Map} with {@code keyclass}/{@code valueclass};</li>
     *   <li>nested data — {@code row(...)}, {@code array(row(...))} becomes a {@link String} marked {@code json=true},
     *       and {@link TrinoFeatureReader} renders the value as JSON.</li>
     * </ul>
     *
     * <p>{@code decimal} becomes a plain {@link String}, having no GeoTools equivalent- {@code ObjectType} has
     * no entry for {@link java.math.BigDecimal}, and {@code Double} only supports 17 significant digits —
     * {@code decimal(38,10)} addresses that. Note that a {@code decimal} nested inside structural data is treated differently,
     * staying an unquoted JSON number; see {@link TrinoTypeSignature#scalarBinding}.
     *
     * <p>Any other unmapped SQL type keeps falls back to byte[].
     *
     * @param name column name
     * @param sqlType {@link java.sql.Types} code
     * @param typeName parameterized Trino type name, or null when unavailable
     * @param isGeometry whether the column was identified as a geometry
     * @param geometryBinding resolved JTS subtype for a geometry column
     * @param srid geometry SRID, 0 when not applicable
     * @return the attribute descriptor
     */
    static AttributeDescriptor toDescriptor(String name, int sqlType, String typeName,
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
            case Types.VARCHAR, Types.LONGNVARCHAR, Types.NVARCHAR  -> String.class;
            case Types.BIGINT                                       -> Long.class;
            case Types.INTEGER, Types.SMALLINT, Types.TINYINT       -> Integer.class;
            case Types.DOUBLE, Types.FLOAT, Types.REAL              -> Double.class;
            case Types.DECIMAL, Types.NUMERIC                       -> String.class;
            case Types.BOOLEAN, Types.BIT                           -> Boolean.class;
            case Types.TIMESTAMP, Types.TIMESTAMP_WITH_TIMEZONE     -> Date.class;
            case Types.BINARY, Types.VARBINARY, Types.LONGVARBINARY -> byte[].class;
            default                                                 -> null;
        };
        if (binding != null) {
            b.setBinding(binding);
            return b.buildDescriptor(name);
        }
        return structuralDescriptor(b, name, sqlType, typeName);
    }

    /**
     *  Descriptor for a structural column. Parsing of the type signature lives in {@link TrinoTypeSignature}.
     */
    private static AttributeDescriptor structuralDescriptor(AttributeTypeBuilder b, String name,
                                                            int sqlType, String typeName) {
        String t = TrinoTypeSignature.normalize(typeName);

        String element = TrinoTypeSignature.arrayElement(t);
        if (element != null) {
            Class<?> binding = TrinoTypeSignature.scalarBinding(element);
            if (binding != null) {
                b.setBinding(List.class);
                b.userData(OPT_SUBTYPE, binding.getName());
                return b.buildDescriptor(name);
            }
            return jsonDescriptor(b, name, typeName);  // array of row/array/map
        }

        String[] kv = TrinoTypeSignature.mapKeyValue(t);
        if (kv != null) {
            Class<?> key = TrinoTypeSignature.scalarBinding(kv[0]);
            Class<?> value = TrinoTypeSignature.scalarBinding(kv[1]);
            if (key != null && value != null) {
                b.setBinding(Map.class);
                b.userData(OPT_KEY_CLASS, key.getName());
                b.userData(OPT_VALUE_CLASS, value.getName());
                return b.buildDescriptor(name);
            }
            return jsonDescriptor(b, name, typeName);  // map with a structural side
        }

        if (TrinoTypeSignature.isRowOrVariant(t)) {
            return jsonDescriptor(b, name, typeName);
        }

        // Unrecognized type fallback.
        LOG.warn("No GeoTools binding for SQL type {}{} on column '{}'; exposing it as byte[] (opaque).",
                 sqlType, t.isEmpty() ? "" : " ('" + t + "')", name);
        b.setBinding(byte[].class);
        return b.buildDescriptor(name);
    }

    /**
     * A String attribute flagged {@code json=true}; the reader renders the value as JSON.
     *
     * <p>When the shape can be derived from the type signature it is also declared under
     * {@code json-schema}, so that a schema discovered here survives being written back out
     * through a store that supports structural fields. {@code typeName} is passed raw:
     * {@link TrinoTypeSignature#normalize} would lower-case quoted field names, and the JSON
     * keys matched against them are case-sensitive.
     */
    private static AttributeDescriptor jsonDescriptor(AttributeTypeBuilder b, String name,
                                                      String typeName) {
        b.setBinding(String.class);
        b.userData(OPT_JSON, "true");
        TrinoAvroSchema.of(name, typeName).ifPresent(schema -> b.userData(OPT_JSON_SCHEMA, schema));
        return b.buildDescriptor(name);
    }
}
