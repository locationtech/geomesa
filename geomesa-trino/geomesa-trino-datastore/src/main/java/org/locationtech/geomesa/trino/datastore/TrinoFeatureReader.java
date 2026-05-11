/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.trino.datastore;

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

import java.io.IOException;
import java.sql.*;
import java.util.Date;
import java.util.NoSuchElementException;

class TrinoFeatureReader implements FeatureReader<SimpleFeatureType, SimpleFeature> {

    private final SimpleFeatureType sft;
    private final Connection conn;
    private final Statement stmt;
    private final ResultSet rs;
    private final WKBReader wkbReader = new WKBReader();
    private final SimpleFeatureBuilder builder;
    private final String fidColumn;
    private final String visColumn;
    private Boolean hasNext = null;

    TrinoFeatureReader(SimpleFeatureType sft, Connection conn,
                       Statement stmt, ResultSet rs, String fidColumn, String visColumn) {
        this.sft       = sft;
        this.conn      = conn;
        this.stmt      = stmt;
        this.rs        = rs;
        this.builder   = new SimpleFeatureBuilder(sft);
        this.fidColumn = fidColumn;
        this.visColumn = visColumn;
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

        String fid = null;
        String vis = null;
        try {
            String fidStr = rs.getString(fidColumn);
            if (!rs.wasNull()) fid = fidStr;

            for (int i = 0; i < sft.getAttributeCount(); i++) {
                AttributeDescriptor desc = sft.getDescriptor(i);
                String col = desc.getLocalName();

                Object value;
                if (Geometry.class.isAssignableFrom(desc.getType().getBinding())) {
                    byte[] wkb = rs.getBytes(col);
                    value = wkb != null ? wkbReader.read(wkb) : null;
                } else if (Date.class.isAssignableFrom(desc.getType().getBinding())) {
                    Timestamp ts = rs.getTimestamp(col);
                    value = ts != null ? new Date(ts.getTime()) : null;
                } else {
                    value = rs.getObject(col);
                }

                builder.set(col, value);
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
