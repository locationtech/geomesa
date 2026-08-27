/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.trino.datastore;

import io.trino.jdbc.Row;
import io.trino.jdbc.RowField;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import org.geotools.api.data.FeatureReader;
import org.geotools.api.feature.IllegalAttributeException;
import org.geotools.api.feature.simple.SimpleFeature;
import org.geotools.api.feature.simple.SimpleFeatureType;
import org.geotools.api.feature.type.AttributeDescriptor;
import org.geotools.feature.simple.SimpleFeatureBuilder;
import org.locationtech.geomesa.security.SecurityUtils;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.io.ParseException;
import org.locationtech.jts.io.WKBReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.*;
import java.util.*;
import java.util.Date;
import java.util.stream.IntStream;

class TrinoFeatureReader implements FeatureReader<SimpleFeatureType, SimpleFeature> {

    private static final Logger LOG = LoggerFactory.getLogger(TrinoFeatureReader.class);

    private final SimpleFeatureType sft;
    private final Connection conn;
    private final Statement stmt;
    private final ResultSet rs;
    private final WKBReader wkbReader = new WKBReader();

    /** Renders structural values for {@code json=true} attributes. */
    private static final ObjectMapper JSON = new ObjectMapper()
            .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
    private final SimpleFeatureBuilder builder;
    private final String fidColumn;
    private final String visColumn;
    /** Per-attribute lookups, resolved once instead of per row. */
    private final Column[] columns;
    private Boolean hasNext = null;
    private long nextId = 0;
    private enum Kind { GEOMETRY, DATE, STRUCTURAL, JSON, PLAIN }

    /**
     * An attribute's name, position and handling, all of which are fixed for the life of the reader.
     *
     * @param index position in the feature type
     * @param name column name
     * @param kind how to convert the raw JDBC value
     */
    private record Column(int index, String name, Kind kind) {
        static Column of(int index, AttributeDescriptor desc) {
            return new Column(index, desc.getLocalName(), kindOf(desc));
        }
    }

    TrinoFeatureReader(SimpleFeatureType sft, Connection conn,
                       Statement stmt, ResultSet rs, String fidColumn, String visColumn) {
        this.sft       = sft;
        this.conn      = conn;
        this.stmt      = stmt;
        this.rs        = rs;
        this.builder   = new SimpleFeatureBuilder(sft);
        this.fidColumn = fidColumn;
        this.visColumn = visColumn;
        this.columns   = IntStream.range(0, sft.getAttributeCount())
                             .mapToObj(i -> Column.of(i, sft.getDescriptor(i)))
                             .toArray(Column[]::new);
    }

    /**
     * Classifies an attribute by the conversion its declared binding needs.
     *
     * @param desc the attribute
     * @return the handling to apply to every value read for it
     */
    private static Kind kindOf(AttributeDescriptor desc) {
        Class<?> binding = desc.getType().getBinding();
        if (Geometry.class.isAssignableFrom(binding)) return Kind.GEOMETRY;
        if (Date.class.isAssignableFrom(binding)) return Kind.DATE;
        if (List.class.isAssignableFrom(binding) || Map.class.isAssignableFrom(binding)) return Kind.STRUCTURAL;
        if (String.class.equals(binding) && isJson(desc)) return Kind.JSON;
        return Kind.PLAIN;
    }

    /**
     * Whether the attribute is marked {@code json=true}, i.e. its string value is a rendered JSON document.
     */
    private static boolean isJson(AttributeDescriptor desc) {
        return Boolean.parseBoolean(String.valueOf(desc.getUserData().get(TrinoTypeMapper.OPT_JSON)));
    }

    /**
     * Returns the feature type of the features produced by this reader.
     *
     * @return feature type
     */
    @Override
    public SimpleFeatureType getFeatureType() { return sft; }

    /**
     * Whether another feature is available.
     *
     * @return true if another feature can be read
     */
    @Override
    public boolean hasNext() throws IOException {
        if (hasNext == null) {
            try {
                hasNext = rs.next();
            } catch (SQLException e) {
                throw new IOException(e);
            }
        }
        return hasNext;
    }

    /**
     * Reads and returns the next feature.
     *
     * @return the next feature
     */
    @Override
    public SimpleFeature next() throws IOException, IllegalAttributeException, NoSuchElementException {
        if (!hasNext()) throw new NoSuchElementException();
        hasNext = null;

        String fid;
        String vis = null;
        try {
            // note: rs.getString should never return null here, but if it does SimpleFeatureBuilder will handle it
            fid = fidColumn == null ? Long.toString(nextId++) : rs.getString(fidColumn);

            for (Column column : columns) {
                String col = column.name();
                Object value;
                switch (column.kind()) {
                    case GEOMETRY -> {
                        byte[] wkb = rs.getBytes(col);
                        value = wkb != null ? wkbReader.read(wkb) : null;
                    }
                    case DATE -> {
                        Timestamp ts = rs.getTimestamp(col);
                        value = ts != null ? new Date(ts.getTime()) : null;
                    }
                    default -> value = coerce(rs.getObject(col), column);
                }

                builder.set(column.index(), value);
            }
            if (visColumn != null) {
                vis = rs.getString(visColumn);
            }
        } catch (SQLException | ParseException e) {
            throw new IOException("Failed to read row", e);
        }

        SimpleFeature feature = builder.buildFeature(fid);
        if (vis != null && !vis.isEmpty()) {
            SecurityUtils.setFeatureVisibility(feature, vis);
        }
        return feature;
    }

    /**
     * Brings a structural JDBC value in line with the attribute's declared binding.
     *
     * <p>The Trino driver hands back {@code java.sql.Array} for arrays and {@link Map} for maps, with {@code row}
     * values arriving as maps of field name to value. Historically these were declared {@code byte[]} and passed through
     * uncoerced, which happened to work but promised the wrong type to anything reading the schema.
     *
     * @param value raw JDBC value
     * @param column the attribute it belongs to
     * @return a value assignable to the declared binding
     */
    private Object coerce(Object value, Column column) throws SQLException {
        if (value == null) {
            return null;
        }
        if (column.kind() == Kind.STRUCTURAL) {
            return normalize(value);
        }
        if (column.kind() == Kind.JSON && !(value instanceof String)) {
            Object plain = normalize(value);
            try {
                return JSON.writeValueAsString(plain);
            } catch (Exception e) {
                // One unrenderable value must not abort the scan, but emitting non-JSON into a json=true attribute
                // would break downstream consumers, so drop it and warn.
                LOG.warn("Could not render column '{}' as JSON; returning null", column.name(), e);
                return null;
            }
        }
        return value;
    }

    /**
     * Recursively rewrites driver-specific containers into plain JDK types.
     *
     * <p>{@code java.sql.Array} becomes a {@link List}, {@code row} a {@link Map} keyed by field name.
     * Both nest cleanly: i.e. {@code array(row(... array(row(...))))}, handled via recursion.
     *
     * <p>The driver models {@code row} as {@link Row} and never as {@code java.sql.Struct}
     * ({@code TrinoConnection.createStruct} is unsupported), so there is no {@code Struct} branch here.
     * n.b. package-private for testing
     */
    static Object normalize(Object value) throws SQLException {
        if (value instanceof Row row) {
            Map<Object, Object> out = new LinkedHashMap<>();
            List<RowField> fields = row.getFields();
            for (RowField f : fields) {
                out.put(f.getName().orElseGet(() -> "field" + f.getOrdinal()),
                        normalize(f.getValue()));
            }
            return out;
        }
        if (value instanceof Array array) {
            Object raw = array.getArray();
            if (raw == null) {
                return null;
            }
            if (raw instanceof Object[] elements) {
                List<Object> out = new ArrayList<>(elements.length);
                for (Object e : elements) {
                    out.add(normalize(e));
                }
                return out;
            }
            LOG.warn("Expected an Object[] payload from column of type '{}' but got {}; passing it through uncoerced",
                    array.getBaseTypeName(), raw.getClass().getName());
            return raw;
        }
        if (value instanceof List<?> list) {
            List<Object> out = new ArrayList<>(list.size());
            for (Object e : list) {
                out.add(normalize(e));
            }
            return out;
        }
        if (value instanceof Map<?, ?> map) {
            Map<Object, Object> out = new LinkedHashMap<>();
            for (Map.Entry<?, ?> e : map.entrySet()) {
                out.put(e.getKey(), normalize(e.getValue()));
            }
            return out;
        }
        return value;
    }

    /**
     * Closes the underlying result set, statement, and connection.
     */
    @Override
    public void close() throws IOException {
        try {
            rs.close();
        } catch (SQLException rsEx) {
            try {
                stmt.close();
            } catch (SQLException stmtEx) {
                rsEx.addSuppressed(stmtEx);
            }
            try {
                conn.close();
            } catch (SQLException connEx) {
                rsEx.addSuppressed(connEx);
            }
            throw new IOException(rsEx);
        }
        try {
            stmt.close();
        } catch (SQLException stmtEx) {
            try {
                conn.close();
            } catch (SQLException connEx) {
                stmtEx.addSuppressed(connEx);
            }
            throw new IOException(stmtEx);
        }
        try {
            conn.close();
        } catch (SQLException e) {
            throw new IOException(e);
        }
    }
}
