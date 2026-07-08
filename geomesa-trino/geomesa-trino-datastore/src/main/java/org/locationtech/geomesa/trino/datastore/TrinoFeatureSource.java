/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.trino.datastore;

import org.geotools.api.data.FeatureReader;
import org.geotools.api.data.Query;
import org.geotools.api.feature.simple.SimpleFeature;
import org.geotools.api.feature.simple.SimpleFeatureType;
import org.geotools.api.filter.Filter;
import org.geotools.api.filter.expression.PropertyName;
import org.geotools.api.filter.sort.SortBy;
import org.geotools.api.filter.sort.SortOrder;
import org.geotools.data.store.ContentEntry;
import org.geotools.data.store.ContentFeatureSource;
import org.geotools.feature.simple.SimpleFeatureTypeBuilder;
import org.geotools.geometry.jts.ReferencedEnvelope;
import org.geotools.data.jdbc.FilterToSQLException;
import org.geotools.referencing.crs.DefaultGeographicCRS;

import org.locationtech.geomesa.security.AuthorizationsProvider;

import static org.locationtech.geomesa.trino.datastore.TrinoDataStore.escapeQuotes;

import java.io.IOException;
import java.sql.*;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.stream.Collectors;

class TrinoFeatureSource extends ContentFeatureSource {

    private static final Logger LOG = LoggerFactory.getLogger(TrinoFeatureSource.class);

    private final TrinoDataStore trinoStore;

    TrinoFeatureSource(ContentEntry entry, TrinoDataStore store) {
        super(entry, null);
        this.trinoStore = store;
    }

    /**
     * All filter evaluation is pushed down to Trino SQL via TrinoFilterToSQL, so the framework
     * should not apply a second Java-level post-filter on top of the reader.
     *
     * @param query the query being planned
     * @return {@code true}; all filtering is handled in SQL
     */
    @Override
    protected boolean canFilter(Query query) {
        return true;
    }

    /**
     * Attributes are projected directly in the SQL SELECT (see getReaderInternal).
     *
     * @param query the query being planned
     * @return {@code true}; attribute projection is handled in SQL
     */
    @Override
    protected boolean canRetype(Query query) {
        return true;
    }

    /**
     * Sorting is pushed down as an ORDER BY in the SQL (see getReaderInternal).
     *
     * @param query the query being planned
     * @return {@code true}; sorting is handled in SQL
     */
    @Override
    protected boolean canSort(Query query) {
        return true;
    }

    /**
     * Max-features is pushed down as a LIMIT in the SQL (see getReaderInternal), so the
     * framework should not re-apply it by truncating the reader. Offsets are NOT pushed
     * down ({@code canOffset} stays false): the framework skips {@code startIndex} rows
     * from the reader client-side, so the pushed-down limit covers them too — see
     * {@link #effectiveLimit(Query)}.
     *
     * @param query the query being planned
     * @return {@code true}; limiting is handled in SQL
     */
    @Override
    protected boolean canLimit(Query query) {
        return true;
    }

    /** The SQL LIMIT for a query: {@code startIndex + maxFeatures}, or -1 when unlimited.
     *  Because the framework applies the {@code startIndex} skip client-side (canOffset
     *  is false), the pushed-down limit must include the rows the framework will skip;
     *  the same value caps {@code getCount} (the framework subtracts {@code startIndex}
     *  from the returned count afterwards). */
    private static long effectiveLimit(Query query) {
        if (query.isMaxFeaturesUnlimited()) {
            return -1;
        }
        int start = query.getStartIndex() != null ? query.getStartIndex() : 0;
        return (long) start + query.getMaxFeatures();
    }

    /**
     * Builds the feature type by discovering the Trino table's schema.
     *
     * @return the discovered simple feature type
     */
    @Override
    protected SimpleFeatureType buildFeatureType() throws IOException {
        String typeName = entry.getName().getLocalPart();
        return new TrinoSchemaDiscovery(trinoStore).discover(typeName);
    }

    /**
     * Counts matching rows via a pushed-down {@code SELECT COUNT(*)}.
     *
     * @param query the query whose filter is translated to SQL
     * @return the matching row count, or -1 if unknown or larger than {@code Integer.MAX_VALUE}
     */
    @Override
    protected int getCountInternal(Query query) throws IOException {
        try {
            return countOnce(query);
        } catch (SQLException e) {
            if (refreshSchemaIfDrifted()) {
                try {
                    return countOnce(query);
                } catch (SQLException retry) {
                    LOG.warn("Failed to execute count query after schema refresh: " + retry.getMessage());
                    return -1;
                }
            }
            LOG.warn("Failed to execute count query: " + e.getMessage());
            return -1;
        }
    }

    private int countOnce(Query query) throws IOException, SQLException {
        String typeName = entry.getName().getLocalPart();
        VisibilityContext vis = visibility();
        String where = combineWhere(encodeFilterSql(query.getFilter()),
            vis == null ? null : vis.conjunct());
        String sql = String.format("SELECT COUNT(*) FROM %s.%s.%s%s",
            escapeQuotes(trinoStore.catalog()), escapeQuotes(trinoStore.trinoSchema()), escapeQuotes(typeName), where);
        try (Connection conn = trinoStore.connect(vis == null ? null : vis.auths());
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {
            rs.next();
            long total = rs.getLong(1);
            // Advance past the last row so the Trino JDBC driver receives the "no more pages"
            // confirmation before close. Otherwise, query appears as cancelled to Trino.
            while (rs.next()) {}
            // canLimit=true disables the framework's min(count, maxFeatures) clamp, so
            // apply it here; startIndex + maxFeatures because the framework subtracts
            // startIndex from the returned count when canOffset is false.
            long cap = effectiveLimit(query);
            if (cap >= 0 && total > cap) {
                total = cap;
            }
            if (total > Integer.MAX_VALUE) {
                LOG.debug("Count " + total + " exceeds Integer.MAX_VALUE; reporting -1 (unknown).");
                return -1;
            }
            return (int) total;
        }
    }

    /**
     * Computes the bounds from the geometry's bbox companion columns.
     *
     * @param query the query whose filter is translated to SQL
     * @return the bounds of matching features, or null if unavailable
     */
    @Override
    protected ReferencedEnvelope getBoundsInternal(Query query) throws IOException {
        try {
            return boundsOnce(query);
        } catch (SQLException e) {
            if (refreshSchemaIfDrifted()) {
                try {
                    return boundsOnce(query);
                } catch (SQLException retry) {
                    LOG.warn("Failed to compute bounds after schema refresh: " + retry.getMessage());
                    return null;
                }
            }
            LOG.warn("Failed to compute bounds for '" + entry.getName().getLocalPart()
                + "': " + e.getMessage());
            return null;
        }
    }

    private ReferencedEnvelope boundsOnce(Query query) throws IOException, SQLException {
        String typeName = entry.getName().getLocalPart();
        if (getSchema().getGeometryDescriptor() == null) return null;
        String geomName = getSchema().getGeometryDescriptor().getLocalName();
        String bboxCol = "__" + geomName + "_bbox__";
        VisibilityContext vis = visibility();
        String where = combineWhere(encodeFilterSql(query.getFilter()),
            vis == null ? null : vis.conjunct());
        String sql = String.format(
            "SELECT MIN(%1$s.xmin), MIN(%1$s.ymin)," +
            " MAX(%1$s.xmax), MAX(%1$s.ymax)" +
            " FROM %2$s.%3$s.%4$s%5$s",
            escapeQuotes(bboxCol), escapeQuotes(trinoStore.catalog()), escapeQuotes(trinoStore.trinoSchema()), escapeQuotes(typeName), where);
        try (Connection conn = trinoStore.connect(vis == null ? null : vis.auths());
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {
            if (rs.next()) {
                double minx = rs.getDouble(1); boolean minxNull = rs.wasNull();
                double miny = rs.getDouble(2); boolean minyNull = rs.wasNull();
                double maxx = rs.getDouble(3); boolean maxxNull = rs.wasNull();
                double maxy = rs.getDouble(4); boolean maxyNull = rs.wasNull();
                while (rs.next()) {} // drain — same reason as getCountInternal
                if (!minxNull && !minyNull && !maxxNull && !maxyNull) {
                    return new ReferencedEnvelope(minx, maxx, miny, maxy,
                        DefaultGeographicCRS.WGS84);
                }
            }
        }
        return null;
    }

    /**
     * Opens a streaming reader over the matching rows, with filter, attribute projection, and
     * sort pushed down to SQL.
     *
     * @param query the query being executed
     * @return a feature reader over the result set
     */
    @Override
    protected FeatureReader<SimpleFeatureType, SimpleFeature> getReaderInternal(Query query)
        throws IOException {
        try {
            return openReader(query);
        } catch (IOException e) {
            // The cached schema may be stale — the table can be dropped/recreated or
            // altered (e.g. a visibility column removed) underneath this long-lived
            // store, leaving the SQL referencing columns that no longer exist. If the
            // live schema differs from the cached one, refresh and retry once.
            if (refreshSchemaIfDrifted()) {
                return openReader(query);
            }
            throw e;
        }
    }

    private FeatureReader<SimpleFeatureType, SimpleFeature> openReader(Query query)
        throws IOException {
        String typeName = entry.getName().getLocalPart();
        String fidColumn = "__fid__";
        VisibilityContext vis = visibility();
        String visColumn = vis == null ? null : vis.visColumn();
        SimpleFeatureType sft = query.retrieveAllProperties()
            ? getSchema()
            : SimpleFeatureTypeBuilder.retype(getSchema(), query.getPropertyNames());
        String cols = escapeQuotes(fidColumn);
        if (sft.getAttributeCount() > 0) {
            cols += ", " + sft.getAttributeDescriptors().stream()
                .map(d -> escapeQuotes(d.getLocalName()))
                .collect(Collectors.joining(", "));
        }
        if (visColumn != null) {
            cols += ", " + escapeQuotes(visColumn);
        }
        // Auths fetched once (in visibility()) so the extra credential and the SQL
        // conjunct can't diverge under per-request providers.
        List<String> auths = vis == null ? null : vis.auths();
        String where = combineWhere(encodeFilterSql(query.getFilter()),
            visColumn == null ? null : visibilityConjunct(visColumn, auths));
        String orderBy = toOrderByClause(query.getSortBy());
        long cap = effectiveLimit(query);
        String limit = cap < 0 ? "" : " LIMIT " + cap;
        String sql = String.format("SELECT %s FROM %s.%s.%s%s%s%s",
            cols, escapeQuotes(trinoStore.catalog()), escapeQuotes(trinoStore.trinoSchema()), escapeQuotes(typeName), where, orderBy, limit);
        Connection conn;
        try {
            conn = trinoStore.connect(auths);
        } catch (SQLException e) {
            throw new IOException("Failed to open connection for query: " + sql, e);
        }
        try {
            Statement stmt = conn.createStatement();
            stmt.setFetchSize(10_000);  // hint; reduces client page round trips
            ResultSet rs   = stmt.executeQuery(sql);
            return new TrinoFeatureReader(sft, conn, stmt, rs, fidColumn, visColumn);
        } catch (Exception e) {
            try { conn.close(); } catch (SQLException suppressed) { e.addSuppressed(suppressed); }
            if (e instanceof RuntimeException re) {
                throw re;
            }
            throw new IOException("Failed to execute query: " + sql, e);
        }
    }

    /**
     * Build an ORDER BY clause from the query's SortBy[]. Sorting on a column not in the SELECT
     * projection is fine — Trino orders by any input column. NATURAL_ORDER / REVERSE_ORDER (and any
     * SortBy with no property name) map to the feature id (__fid__), ascending/descending.
     */
    private String toOrderByClause(SortBy[] sortBy) {
        if (sortBy == null || sortBy.length == 0) return "";
        StringBuilder sb = new StringBuilder(" ORDER BY ");
        for (int i = 0; i < sortBy.length; i++) {
            SortBy s = sortBy[i];
            if (i > 0) sb.append(", ");
            String col = "__fid__";
            SortOrder order = SortOrder.ASCENDING;
            if (s != SortBy.NATURAL_ORDER && s != SortBy.REVERSE_ORDER) {
                PropertyName pn = s.getPropertyName();
                if (pn != null && pn.getPropertyName() != null && !pn.getPropertyName().isEmpty()) {
                    col = pn.getPropertyName();
                }
                if (s.getSortOrder() != null) order = s.getSortOrder();
            } else if (s == SortBy.REVERSE_ORDER) {
                order = SortOrder.DESCENDING;
            }
            sb.append(escapeQuotes(col))
              .append(order == SortOrder.DESCENDING ? " DESC" : " ASC");
        }
        return sb.toString();
    }

    /** SQL conjunct evaluated by the plugin's is_visible UDF. The
     *  column identifier is double-quoted and the auths literal escaped. Throws
     *  {@code IllegalArgumentException} on a token containing a transport delimiter —
     *  the UDF would re-split it into auths that were never issued (see {@link AuthTokens}). */
    static String visibilityConjunct(String visColumn, List<String> auths) {
        AuthTokens.validate(auths);
        String literal = String.join(",", auths).replace("'", "''");
        return "is_visible(" + escapeQuotes(visColumn) + ", '" + literal + "')";
    }

    /** Combine the (possibly null) filter SQL and visibility conjunct into a
     *  WHERE clause; empty string when both are absent. */
    static String combineWhere(String filterSql, String visConjunct) {
        if (filterSql == null && visConjunct == null) return "";
        if (filterSql == null) return " WHERE " + visConjunct;
        if (visConjunct == null) return " WHERE " + filterSql;
        return " WHERE (" + filterSql + ") AND " + visConjunct;
    }

    /** Translate a GeoTools filter to a SQL expression string, or null for null/INCLUDE. */
    private String encodeFilterSql(Filter filter) throws IOException {
        if (filter == null || filter == Filter.INCLUDE) return null;
        try {
            TrinoFilterToSQL toSql = new TrinoFilterToSQL();
            toSql.setFeatureType(getSchema());
            return toSql.encodeToString(filter);
        } catch (FilterToSQLException e) {
            throw new IOException("Failed to translate filter to SQL", e);
        } catch (RuntimeException e) {
            throw new IOException("Unsupported filter type — cannot translate to Trino SQL: " + filter, e);
        }
    }

    /** The table's visibility column plus the caller's auths, captured together so
     *  a query makes a single {@code provider.getAuthorizations()} call and feeds
     *  the same auths to the extra credential and the SQL conjunct. */
    private record VisibilityContext(String visColumn, List<String> auths) {
        String conjunct() {
            return visibilityConjunct(visColumn, auths);
        }
    }

    /** Non-null iff security params are configured AND the table has a visibility
     *  column. Auths are fetched per query (per-request providers). */
    private VisibilityContext visibility() throws IOException {
        AuthorizationsProvider provider = trinoStore.authProvider();
        if (provider == null) return null;
        String visColumn =
            (String) getSchema().getUserData().get(TrinoSchemaDiscovery.VIS_COLUMN_KEY);
        if (visColumn == null) return null;
        return new VisibilityContext(visColumn, provider.getAuthorizations());
    }

    /**
     * Re-discovers the table's live schema and, if it differs from the cached one
     * (attributes or visibility column — the table was dropped/recreated or altered
     * underneath this store), invalidates the GeoTools schema caches so the next
     * {@code getSchema()} rebuilds from the live table. Called only on the failure
     * path, so the extra discovery round trip never taxes a healthy query.
     *
     * @return true if the schema had drifted and the caches were refreshed
     */
    private boolean refreshSchemaIfDrifted() {
        String typeName = entry.getName().getLocalPart();
        try {
            SimpleFeatureType fresh = new TrinoSchemaDiscovery(trinoStore).discover(typeName);
            if (!schemaDrifted(getSchema(), fresh)) {
                return false;
            }
            LOG.warn("Schema for '" + typeName + "' changed underneath the datastore "
                + "(e.g. table recreated or visibility column added/removed); "
                + "refreshing cached schema and retrying");
            entry.getState(getTransaction()).flush();
            schema = null;
            return true;
        } catch (IOException e) {
            LOG.debug("Schema re-discovery for '" + typeName + "' failed: " + e.getMessage());
            return false;
        }
    }

    /** True when the two discovered types differ in attribute names/bindings or in
     *  the visibility column recorded in user data. */
    static boolean schemaDrifted(SimpleFeatureType cached, SimpleFeatureType fresh) {
        Object cachedVis = cached.getUserData().get(TrinoSchemaDiscovery.VIS_COLUMN_KEY);
        Object freshVis  = fresh.getUserData().get(TrinoSchemaDiscovery.VIS_COLUMN_KEY);
        if (!java.util.Objects.equals(cachedVis, freshVis)) {
            return true;
        }
        List<String> cachedAttrs = cached.getAttributeDescriptors().stream()
            .map(d -> d.getLocalName() + ":" + d.getType().getBinding().getName())
            .toList();
        List<String> freshAttrs = fresh.getAttributeDescriptors().stream()
            .map(d -> d.getLocalName() + ":" + d.getType().getBinding().getName())
            .toList();
        return !cachedAttrs.equals(freshAttrs);
    }
}
