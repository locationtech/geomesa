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
import org.geotools.api.filter.And;
import org.geotools.api.filter.Filter;
import org.geotools.api.filter.FilterFactory;
import org.geotools.api.filter.expression.PropertyName;
import org.geotools.api.filter.sort.SortBy;
import org.geotools.api.filter.sort.SortOrder;
import org.geotools.data.FilteringFeatureReader;
import org.geotools.data.MaxFeatureReader;
import org.geotools.data.ReTypeFeatureReader;
import org.geotools.data.jdbc.FilterToSQLException;
import org.geotools.data.store.ContentEntry;
import org.geotools.data.store.ContentFeatureSource;
import org.geotools.factory.CommonFactoryFinder;
import org.geotools.feature.simple.SimpleFeatureTypeBuilder;
import org.geotools.filter.visitor.DefaultFilterVisitor;
import org.geotools.filter.visitor.PropertyNameResolvingVisitor;
import org.geotools.geometry.jts.ReferencedEnvelope;
import org.geotools.referencing.crs.DefaultGeographicCRS;
import org.locationtech.geomesa.index.conf.QueryHints;
import org.locationtech.geomesa.security.AuthorizationsProvider;
import org.locationtech.geomesa.utils.conf.GeoMesaSystemProperties.SystemProperty;
import org.locationtech.geomesa.utils.json.JsonPathParser;
import org.locationtech.geomesa.utils.json.JsonPathParser.PathAttribute;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.collection.JavaConverters;

import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.*;
import java.util.stream.Collectors;

import static org.locationtech.geomesa.trino.datastore.TrinoDataStore.escapeQuotes;

class TrinoFeatureSource extends ContentFeatureSource {

    private static final Logger LOG = LoggerFactory.getLogger(TrinoFeatureSource.class);

    private static final FilterFactory filterFactory = CommonFactoryFinder.getFilterFactory();

    /**
     * Controls whether filter conjuncts that can't be translated to Trino SQL are evaluated
     * client-side (in-memory) or cause the query to fail. Recognized values (case-insensitive):
     * <ul>
     *   <li>{@code partial} (default) — allow client-side evaluation only when at least one
     *       conjunct was pushed down to SQL; a filter with no pushable part fails.</li>
     *   <li>{@code none} — never evaluate filters client-side; any non-pushable conjunct fails
     *       the query.</li>
     *   <li>{@code all} — allow client-side evaluation of any non-pushable conjunct, even when
     *       nothing was pushed down.</li>
     * </ul>
     */
    public static final SystemProperty CLIENT_SIDE_FILTERING =
        new SystemProperty("geomesa.trino.filter.client-side", ClientSideFiltering.PARTIAL.value);

    /** The three client-side filtering behaviors selectable via {@link #CLIENT_SIDE_FILTERING}. */
    enum ClientSideFiltering {
        PARTIAL("partial"), NONE("none"), ALL("all");

        final String value;

        ClientSideFiltering(String value) {
            this.value = value;
        }

        /** Resolve the configured mode, falling back to {@link #PARTIAL} for an unset or
         *  unrecognized value. */
        static ClientSideFiltering current() {
            String configured = CLIENT_SIDE_FILTERING.get();
            for (ClientSideFiltering mode : values()) {
                if (mode.value.equalsIgnoreCase(configured)) {
                    return mode;
                }
            }
            LOG.warn("Unrecognized value '" + configured + "' for " + CLIENT_SIDE_FILTERING.property()
                + "; defaulting to '" + PARTIAL.value + "'");
            return PARTIAL;
        }
    }

    private final TrinoDataStore trinoStore;

    TrinoFeatureSource(ContentEntry entry, TrinoDataStore store) {
        super(entry, null);
        this.trinoStore = store;
    }

    /**
     * Filtering is pushed down to Trino SQL via TrinoFilterToSQL. When every conjunct is
     * pushable this returns {@code true} and the framework applies no second Java-level
     * post-filter. When the filter has a non-pushable conjunct (a "residual"), it returns
     * {@code false} so the framework wraps a {@code FilteringFeatureReader} that re-applies
     * the <em>whole</em> filter client-side on top of the partial SQL pushdown (re-applying
     * the already-pushed conjuncts is harmless and keeps the result correct).
     *
     * @param query the query being planned
     * @return {@code true} when the entire filter pushes to SQL, {@code false} otherwise
     */
    @Override
    protected boolean canFilter(Query query) {
        return true;
    }

    /**
     * Attributes are projected directly in the SQL SELECT (see getReaderInternal). When the
     * filter has a residual, the projection is expanded to include the columns the residual
     * references, so retyping back down to the requested attributes must happen client-side
     * ({@code false}); otherwise the SQL projection already matches ({@code true}).
     *
     * @param query the query being planned
     * @return {@code false} when the filter has a residual, else {@code true}
     */
    @Override
    protected boolean canRetype(Query query) {
        return true;
    }

    /**
     * Sorting is pushed down as an ORDER BY in the SQL (see getReaderInternal). The SQL sort
     * order is preserved by the streaming {@code FilteringFeatureReader}, so this stays
     * {@code true} even when the filter has a residual.
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
     * <p>When the filter has a residual, the LIMIT cannot be pushed down (it would truncate
     * rows before the client-side filter runs, dropping valid matches), so this returns
     * {@code false} and the framework applies the limit after filtering.
     *
     * @param query the query being planned
     * @return {@code false} when the filter has a residual, else {@code true}
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
     * Resolves filter property names against the schema, but preserves JSON paths.
     *
     * <p>The base class runs {@code DataUtilities.resolvePropertyNames}, whose
     * {@link org.geotools.filter.visitor.PropertyNameResolvingVisitor} evaluates each property
     * name against the feature type and rewrites it to the resolved attribute's local name. A
     * JSON path like {@code $.props.name} evaluates (via GeoMesa's JSON property accessor) to
     * the {@code props} descriptor, so the default would collapse the whole path to {@code
     * "props"} — losing the nested field {@code TrinoFilterToSQL} needs to emit a ROW
     * dereference. We keep {@code $}-prefixed names verbatim and resolve everything else as
     * usual.
     *
     * @param query the query being planned
     * @return the query with non-JSON-path property names resolved
     */
    @Override
    protected Query resolvePropertyNames(Query query) {
        Filter filter = query.getFilter();
        if (filter == null || filter == Filter.INCLUDE || filter == Filter.EXCLUDE) {
            return query;
        }
        Filter resolved = (Filter) filter.accept(new JsonPathPreservingResolver(getSchema()), null);
        if (resolved == filter) {
            return query;
        }
        Query newQuery = new Query(query);
        newQuery.setFilter(resolved);
        return newQuery;
    }

    /** Resolves property names against the schema, but leaves {@code $}-prefixed JSON paths
     *  untouched so {@code TrinoFilterToSQL} can translate them into ROW dereferences. */
    private static final class JsonPathPreservingResolver extends PropertyNameResolvingVisitor {
        JsonPathPreservingResolver(SimpleFeatureType featureType) {
            super(featureType);
        }
        @Override
        public Object visit(PropertyName expression, Object extraData) {
            String name = expression.getPropertyName();
            if (name != null && name.startsWith("$")) {
                return getFactory(extraData).property(name);
            }
            return super.visit(expression, extraData);
        }
    }

    /**
     * Builds the feature type by discovering the Trino table's schema.
     *
     * @return the discovered simple feature type
     */
    @Override
    protected SimpleFeatureType buildFeatureType() throws IOException {
        String typeName = entry.getName().getLocalPart();
        String tableName = trinoStore.getTableName(typeName);
        return new TrinoSchemaDiscovery(trinoStore).discover(typeName, tableName);
    }

    /**
     * Counts matching rows via a pushed-down {@code SELECT COUNT(*)}.
     *
     * @param query the query whose filter is translated to SQL
     * @return the matching row count, or -1 if unknown or larger than {@code Integer.MAX_VALUE}
     */
    @Override
    protected int getCountInternal(Query query) throws IOException {
        if (splitFilter(query.getFilter()).residual != null) {
            // part of the filter is evaluated client-side, so SQL COUNT(*) would over-count; return unknown
            return -1;
        }
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
            escapeQuotes(trinoStore.catalog()), escapeQuotes(trinoStore.trinoSchema()), escapeQuotes(trinoStore.getTableName(typeName)), where);
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
        // note: this may give a larger bounds due to not taking client-side filters into account
        String where = combineWhere(encodeFilterSql(query.getFilter()),
            vis == null ? null : vis.conjunct());
        String sql = String.format(
            "SELECT MIN(%1$s.xmin), MIN(%1$s.ymin)," +
            " MAX(%1$s.xmax), MAX(%1$s.ymax)" +
            " FROM %2$s.%3$s.%4$s%5$s",
            escapeQuotes(bboxCol), escapeQuotes(trinoStore.catalog()), escapeQuotes(trinoStore.trinoSchema()), escapeQuotes(trinoStore.getTableName(typeName)), where);
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
        var includeFids = (Boolean) query.getHints().getOrDefault(QueryHints.INCLUDE_FID(), Boolean.TRUE);
        String fidColumn = includeFids ? "__fid__" : null;
        VisibilityContext vis = visibility();
        String visColumn = vis == null ? null : vis.visColumn();
        FilterSplit split = splitFilter(query.getFilter());
        SimpleFeatureType sft;
        boolean retype = false; // if we need to re-type post query
        if (query.retrieveAllProperties()) {
            sft = getSchema();
        } else if (split.residual != null && !split.residualAttributes().isEmpty()) {
            // the client-side residual reads columns that may not be in the requested
            // projection, so pull those too - then retype back down to the requested attributes afterward
            Set<String> names = new LinkedHashSet<>();
            Collections.addAll(names, query.getPropertyNames());
            retype = names.addAll(split.residualAttributes());
            sft = SimpleFeatureTypeBuilder.retype(getSchema(), names.toArray(new String[0]));
        } else {
            sft = SimpleFeatureTypeBuilder.retype(getSchema(), query.getPropertyNames());
        }
        String cols = fidColumn == null ? "" : escapeQuotes(fidColumn);
        if (sft.getAttributeCount() > 0) {
            if (!cols.isEmpty()) {
                cols += ", ";
            }
            cols += sft.getAttributeDescriptors().stream()
                .map(d -> escapeQuotes(d.getLocalName()))
                .collect(Collectors.joining(", "));
        }
        if (visColumn != null) {
            if (!cols.isEmpty()) {
                cols += ", ";
            }
            cols += escapeQuotes(visColumn);
        }
        if (cols.isEmpty()) {
            LOG.debug("Selecting __fid__ column as no columns are selected");
            cols = "__fid__";
        }
        // Auths fetched once (in visibility()) so the extra credential and the SQL
        // conjunct can't diverge under per-request providers.
        List<String> auths = vis == null ? null : vis.auths();
        String where = combineWhere(split.pushableSql, visColumn == null ? null : visibilityConjunct(visColumn, auths));
        String orderBy = toOrderByClause(query.getSortBy());
        // don't push the limit down when there's a residual: the client-side filter runs
        // after the SQL, so a SQL LIMIT could truncate rows before they're evaluated
        long cap = effectiveLimit(query);
        String limit = cap < 0 || split.residual != null ? "" : " LIMIT " + cap;
        String sql = String.format("SELECT %s FROM %s.%s.%s%s%s%s",
            cols, escapeQuotes(trinoStore.catalog()), escapeQuotes(trinoStore.trinoSchema()), escapeQuotes(trinoStore.getTableName(typeName)), where, orderBy, limit);
        Connection conn;
        try {
            conn = trinoStore.connect(auths);
        } catch (SQLException e) {
            throw new IOException("Failed to open connection for query: " + sql, e);
        }

        FeatureReader<SimpleFeatureType, SimpleFeature> reader;
        try {
            Statement stmt = conn.createStatement();
            stmt.setFetchSize(10_000);  // hint; reduces client page round trips
            ResultSet rs   = stmt.executeQuery(sql);
            reader = new TrinoFeatureReader(sft, conn, stmt, rs, fidColumn, visColumn);
        } catch (Exception e) {
            try { conn.close(); } catch (SQLException suppressed) { e.addSuppressed(suppressed); }
            if (e instanceof RuntimeException re) {
                throw re;
            }
            throw new IOException("Failed to execute query: " + sql, e);
        }
        // account for client-side filtering
        if (split.residual != null) {
            reader = new FilteringFeatureReader<>(reader, split.residual);
        }
        if (retype) {
            var target = SimpleFeatureTypeBuilder.retype(getSchema(), query.getPropertyNames());
            reader = new ReTypeFeatureReader(reader, target, false);
        }
        if (cap >= 0 && cap < Integer.MAX_VALUE && split.residual != null) {
            reader = new MaxFeatureReader<>(reader, (int) cap); // TODO is this a safe cast?
        }
        return reader;
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

    /** Translate the pushable conjuncts of a GeoTools filter to a SQL expression string, or
     *  null when nothing is pushable (null/INCLUDE, or every conjunct is a residual). Any
     *  non-pushable conjunct is omitted here and re-applied client-side (see {@link
     *  #canFilter(Query)}). */
    private String encodeFilterSql(Filter filter) {
        return splitFilter(filter).pushableSql();
    }

    /**
     * Splits a filter into the part that can be pushed to Trino SQL and a client-side
     * residual. The top-level filter is decomposed into AND conjuncts; each is trial-encoded
     * with a fresh {@link TrinoFilterToSQL}. Conjuncts that encode cleanly are joined into the
     * pushable SQL; conjuncts the translator rejects become the residual, to be evaluated
     * in-memory by the framework's {@code FilteringFeatureReader}.
     *
     * <p>Whether a residual is allowed is governed by {@link #CLIENT_SIDE_FILTERING}: mode
     * {@code none} rejects any residual, and mode {@code partial} (the default) rejects a
     * residual when nothing was pushed down to SQL. In those cases this throws an
     * {@link IllegalArgumentException} naming the offending conjuncts, failing the query rather
     * than silently evaluating (or not evaluating) it client-side.
     *
     * @param filter the query filter (may be null / INCLUDE)
     * @return the split; {@link FilterSplit#residual} is null when everything pushes down
     */
    private FilterSplit splitFilter(Filter filter) {
        if (filter == null || filter == Filter.INCLUDE) {
            return new FilterSplit(null, null, Collections.emptySet());
        }
        List<Filter> conjuncts;
        if (filter instanceof And and) {
            conjuncts = and.getChildren();
        } else {
            conjuncts = Collections.singletonList(filter);
        }
        List<String> pushable = new ArrayList<>();
        List<Filter> residual = new ArrayList<>();
        for (Filter conjunct : conjuncts) {
            try {
                TrinoFilterToSQL toSql = new TrinoFilterToSQL();
                toSql.setFeatureType(getSchema());
                pushable.add(toSql.encodeToString(conjunct));
            } catch (FilterToSQLException | RuntimeException e) {
                LOG.debug("Cannot push filter conjunct to Trino SQL: " + conjunct + " (" + e.getMessage() + ")");
                residual.add(conjunct);
            }
        }
        String pushableSql = null;
        if (!pushable.isEmpty()) {
            if (pushable.size() == 1) {
                pushableSql = pushable.get(0);
            } else {
                pushableSql = "(" + String.join(") AND (", pushable) + ")";
            }
        }
        if (residual.isEmpty()) {
            return new FilterSplit(pushableSql, null, Collections.emptySet());
        }
        ClientSideFiltering mode = ClientSideFiltering.current();
        if (mode == ClientSideFiltering.NONE) {
            throw new IllegalArgumentException("Cannot push the following filter(s) to Trino SQL and client-side "
                + "filtering is disabled (" + CLIENT_SIDE_FILTERING.property() + "=" + mode.value + "): " + residual);
        }
        if (mode == ClientSideFiltering.PARTIAL && pushableSql == null) {
            throw new IllegalArgumentException("Cannot push any part of the filter to Trino SQL and client-side "
                + "filtering requires a partial pushdown (" + CLIENT_SIDE_FILTERING.property() + "=" + mode.value
                + "): " + residual);
        }
        LOG.warn("Cannot push the following filter(s) to Trino SQL, evaluating client-side: " + residual);
        Filter residualAnd = residual.size() == 1 ? residual.get(0) : filterFactory.and(residual);
        Set<String> residualAttributes = residualAttributeNames(residualAnd);
        return new FilterSplit(pushableSql, residualAnd, residualAttributes);
    }

    /**
     * The backing schema attributes a residual conjunct references, used to expand the SELECT
     * projection so the client-side filter can read them. JSON paths resolve to their head
     * attribute (e.g. {@code "$.props.tags[0]"} → {@code props}); everything else resolves via
     * {@code DataUtilities.attributeNames}. Throws when a JSON path's head is ambiguous — a
     * top-level wildcard/deep-scan or a non-existent attribute — because we can't determine a
     * single column to pull without selecting all attributes.
     *
     * @param conjunct a residual filter conjunct
     * @return the set of backing attribute names the conjunct reads
     */
    private Set<String> residualAttributeNames(Filter conjunct) {
        Set<String> names = new LinkedHashSet<>();
        conjunct.accept(new DefaultFilterVisitor() {
            @Override
            public Object visit(PropertyName expression, Object data) {
                String name = expression.getPropertyName();
                if (name != null && name.startsWith("$")) {
                    names.add(jsonPathHeadAttribute(name));
                } else if (name != null && !name.isEmpty()) {
                    names.add(name);
                }
                return data;
            }
        }, null);
        return names;
    }

    /**
     * Resolves the head attribute of a JSON path (the element that selects the SFT attribute),
     * throwing if it is anything other than a concrete, existing attribute name.
     *
     * @param pathString a {@code $}-prefixed JSON path
     * @return the backing attribute name the path reads from
     */
    private String jsonPathHeadAttribute(String pathString) {
        JsonPathParser.JsonPath path;
        try {
            path = JsonPathParser.parse(pathString, false);
        } catch (RuntimeException e) {
            throw new IllegalArgumentException("Could not evaluate attribute as a JSON path: " + pathString, e);
        }
        if (path.isEmpty() || !(JavaConverters.seqAsJavaList(path.elements()).get(0) instanceof PathAttribute head)) {
            throw new IllegalArgumentException("Invalid JSON path - first element must point at a named attribute "
                + "(top-level wildcards and deep scans are ambiguous): " + pathString);
        }
        if (getSchema().getDescriptor(head.name()) == null) {
            throw new IllegalArgumentException("Invalid JSON path - does not point at an attribute: " + pathString);
        }
        return head.name();
    }

    /** The result of splitting a filter into a pushed-down SQL fragment and a client-side
     *  residual. {@code pushableSql} is null when nothing pushes down; {@code residualAttributes}
     *  is the set of backing columns the residual reads (empty when there's no residual). */
    private record FilterSplit(String pushableSql, Filter residual, Set<String> residualAttributes) {}

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
            SimpleFeatureType fresh = new TrinoSchemaDiscovery(trinoStore).discover(typeName, trinoStore.getTableName(typeName));
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
