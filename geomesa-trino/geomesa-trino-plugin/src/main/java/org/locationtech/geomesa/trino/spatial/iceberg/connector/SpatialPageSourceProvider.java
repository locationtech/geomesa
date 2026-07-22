/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.trino.spatial.iceberg.connector;

import io.trino.plugin.iceberg.IcebergColumnHandle;
import io.trino.plugin.iceberg.IcebergTableHandle;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.ConnectorPageSourceProvider;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.connector.ConnectorTableCredentials;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.DynamicFilter;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.Range;
import io.trino.spi.predicate.SortedRangeSet;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.type.RealType;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.locationtech.geomesa.trino.spatial.iceberg.connector.BboxFilteringPageSource.AcceptConfig;
import org.locationtech.geomesa.trino.spatial.iceberg.connector.BboxFilteringPageSource.BboxBound;

import static org.locationtech.geomesa.trino.spatial.iceberg.connector.SpatialTableHandle.XMIN;
import static org.locationtech.geomesa.trino.spatial.iceberg.connector.SpatialTableHandle.YMIN;

/**
 * Wraps the Iceberg page-source provider to apply the connector's bbox filtering in
 * {@link BboxFilteringPageSource}. Two engagements, both driven by the bbox reject box:
 *
 * <ul>
 *   <li><strong>Short-circuit</strong> — the handle is a {@link SpatialTableHandle} (the connector
 *       claimed a rectangle {@code ST_Intersects} on a point column enforced). Reads the bbox
 *       sub-fields + geometry and does the authoritative reject/accept/exact filtering; the reject
 *       box is built from the exact rectangle with outward float rounding.</li>
 *   <li><strong>Reject-only</strong> — a plain {@link IcebergTableHandle} whose unenforced predicate
 *       carries the connector-injected REAL bbox domains (any spatial predicate that could not be
 *       claimed enforced: non-rectangle query, non-point column, {@code ST_Contains}, …). Rebuilds
 *       the same reject box <em>from those domains</em> and drops rows that cannot overlap before the
 *       engine's exact {@code ST_*} residual decodes their WKB.</li>
 * </ul>
 *
 * <p>Otherwise (non-spatial query, or no pushed bbox domains) it delegates unchanged — zero overhead.
 * The handle is always unwrapped to its underlying Iceberg handle before the delegate reads it; a
 * missed unwrap would surface as a loud {@code ClassCastException}, never a wrong result.
 */
final class SpatialPageSourceProvider implements ConnectorPageSourceProvider {

    private static final Logger LOG = LoggerFactory.getLogger(SpatialPageSourceProvider.class);
    private static final GeometryFactory GEOMETRY_FACTORY = new GeometryFactory();
    private static final String BBOX_SUFFIX = "_bbox__";

    private final ConnectorPageSourceProvider delegate;

    SpatialPageSourceProvider(ConnectorPageSourceProvider delegate) {
        this.delegate = delegate;
    }

    @Override
    public ConnectorPageSource createPageSource(
            ConnectorTransactionHandle transaction, ConnectorSession session,
            ConnectorSplit split, ConnectorTableHandle table,
            Optional<ConnectorTableCredentials> credentials,
            List<ColumnHandle> columns, DynamicFilter dynamicFilter) {
        Plan plan = plan(table, columns);
        ConnectorPageSource ps = delegate.createPageSource(
            transaction, session, split, SpatialTableHandle.unwrap(table), credentials,
            plan.columns(), dynamicFilter);
        return plan.wrap(ps);
    }

    @Override
    public ConnectorPageSource createPageSource(
            ConnectorTransactionHandle transaction, ConnectorSession session,
            ConnectorSplit split, ConnectorTableHandle table,
            List<ColumnHandle> columns, DynamicFilter dynamicFilter) {
        Plan plan = plan(table, columns);
        ConnectorPageSource ps = delegate.createPageSource(
            transaction, session, split, SpatialTableHandle.unwrap(table), plan.columns(), dynamicFilter);
        return plan.wrap(ps);
    }

    /** The physical columns to read (query columns, plus the bbox sub-fields — and, in short-circuit
     *  mode, the geometry — appended to evaluate the filter), the reject box, and (short-circuit only)
     *  the accept/exact config. Empty reject box ⇒ nothing to filter, delegate passes through. */
    private record Plan(List<ColumnHandle> columns, int outputChannelCount, boolean stripAddedChannels,
                        List<BboxBound> rejectBounds, AcceptConfig accept) {
        ConnectorPageSource wrap(ConnectorPageSource ps) {
            return rejectBounds.isEmpty() ? ps
                : new BboxFilteringPageSource(ps, outputChannelCount, stripAddedChannels, rejectBounds, accept);
        }
    }

    private Plan plan(ConnectorTableHandle table, List<ColumnHandle> columns) {
        if (table instanceof SpatialTableHandle sth) {
            LOG.debug("bbox short-circuit ENGAGED (reject/accept/exact)");
            return planAccept(sth, columns);
        }
        if (table instanceof IcebergTableHandle ith) {
            return planReject(ith, columns);
        }
        return passthrough(columns);
    }

    private static Plan passthrough(List<ColumnHandle> columns) {
        return new Plan(columns, columns.size(), false, List.of(), null);
    }

    /** Short-circuit: authoritative reject/accept/exact for POINT data — Z2 ⟺ sft subtype
     *  {@code Point}, the only case {@link SpatialConnectorMetadata} wraps in a
     *  {@link SpatialTableHandle}. For a point the stored bbox is degenerate ({@code xmin==xmax==x},
     *  {@code ymin==ymax==y}), so we read only the two lower sub-fields as the point's {@code (x, y)} —
     *  half the bbox columns scanned and half the per-row compares. Box overlap collapses to a
     *  point-in-range test, built from the exact query rectangle with directional 2-ulp float rounding
     *  so any point a nearest-rounded float32 could misclassify falls through to the exact shell test. */
    private Plan planAccept(SpatialTableHandle tableHandle, List<ColumnHandle> columns) {
        List<ColumnHandle> augmented = new ArrayList<>(columns);
        int xCh = channelFor(augmented, tableHandle.bboxLeaves().get(XMIN));   // xmin == xmax == x
        int yCh = channelFor(augmented, tableHandle.bboxLeaves().get(YMIN));   // ymin == ymax == y
        int geomCh = channelFor(augmented, tableHandle.geomColumn());

        double minX = tableHandle.rectMinX(),
               minY = tableHandle.rectMinY(),
               maxX = tableHandle.rectMaxX(),
               maxY = tableHandle.rectMaxY();

        // Reject box (necessary condition): the point lies outside the outward-rounded rectangle.
        List<BboxBound> reject = List.of(
            new BboxBound(xCh, outLow(minX), outHigh(maxX)),
            new BboxBound(yCh, outLow(minY), outHigh(maxY)));
        // Accept box (sufficient condition): the point lies inside the inward-rounded rectangle — keep
        // with no WKB decode.
        List<BboxBound> inner = List.of(
            new BboxBound(xCh, inLow(minX), inHigh(maxX)),
            new BboxBound(yCh, inLow(minY), inHigh(maxY)));

        Geometry queryRect = GEOMETRY_FACTORY.createPolygon(new Coordinate[]{
            new Coordinate(minX, minY), new Coordinate(maxX, minY), new Coordinate(maxX, maxY),
            new Coordinate(minX, maxY), new Coordinate(minX, minY)});

        boolean stripped = augmented.size() > columns.size();
        return new Plan(augmented, columns.size(), stripped, reject, new AcceptConfig(inner, geomCh, queryRect));
    }

    /** Reject-only pre-filter: rebuild the reject box from the connector-injected REAL bbox domains in
     *  the unenforced predicate (the same overlap condition {@link #planAccept} derives from the exact
     *  rectangle) and drop rows that cannot overlap; the engine's exact {@code ST_*} residual runs on
     *  the survivors. No accept/exact config — the engine, not this page source, is authoritative. */
    private Plan planReject(IcebergTableHandle ith, List<ColumnHandle> columns) {
        TupleDomain<IcebergColumnHandle> predicate = ith.getUnenforcedPredicate();
        if (predicate.isNone() || predicate.getDomains().isEmpty()) {
            return passthrough(columns);
        }
        List<ColumnHandle> augmented = new ArrayList<>(columns);
        List<BboxBound> reject = new ArrayList<>();
        for (Map.Entry<IcebergColumnHandle, Domain> e : predicate.getDomains().get().entrySet()) {
            IcebergColumnHandle handle = e.getKey();
            if (!isBboxSubField(handle)) {
                continue;
            }
            // Reject-only only pays when it pre-empts a WKB decode. If the geometry the bbox guards
            // isn't being read (a bbox-only query), the engine already applies the bbox predicate
            // itself, so this pass would be pure overhead — decline it.
            if (!geomColumnRead(handle, columns)) {
                return passthrough(columns);
            }
            float[] span = spanBounds(e.getValue());
            if (span != null) {
                reject.add(new BboxBound(channelFor(augmented, handle), span[0], span[1]));
            }
        }
        if (reject.isEmpty()) {
            return passthrough(columns);
        }
        LOG.debug("bbox reject-only pre-filter on {} bound(s)", reject.size());
        boolean stripped = augmented.size() > columns.size();
        return new Plan(augmented, columns.size(), stripped, reject, null);
    }

    private static int channelFor(List<ColumnHandle> columns, ColumnHandle handle) {
        int i = columns.indexOf(handle);
        if (i >= 0) {
            return i;
        }
        columns.add(handle);
        return columns.size() - 1;
    }

    /** A REAL sub-field of a {@code __X_bbox__} struct column. */
    private static boolean isBboxSubField(IcebergColumnHandle handle) {
        return !handle.isBaseColumn()
            && handle.getType() instanceof RealType
            && handle.getBaseColumnIdentity().getName().endsWith(BBOX_SUFFIX);
    }

    /** True iff the geometry column guarded by this bbox sub-field is among the read columns — i.e.
     *  there is a downstream WKB decode for reject-only to pre-empt. The geometry column {@code X} is
     *  the base of the {@code __X_bbox__} struct. */
    private static boolean geomColumnRead(IcebergColumnHandle bboxSubField, List<ColumnHandle> columns) {
        String bboxName = bboxSubField.getBaseColumnIdentity().getName();               // __X_bbox__
        String geomName = bboxName.substring(0, bboxName.length() - BBOX_SUFFIX.length()); // __X
        if (geomName.startsWith("__")) {
            geomName = geomName.substring(2);                                           // X
        }
        for (ColumnHandle c : columns) {
            if (c instanceof IcebergColumnHandle ich
                    && ich.isBaseColumn()
                    && ich.getBaseColumnIdentity().getName().equals(geomName)) {
                return true;
            }
        }
        return false;
    }

    /** {@code [low, high]} float bounds of the domain's span (±∞ when unbounded), or null when the
     *  domain is not an ordered range set or imposes no bound. */
    private static float[] spanBounds(Domain domain) {
        if (!(domain.getValues() instanceof SortedRangeSet ranges) || ranges.getRangeCount() == 0) {
            return null;
        }
        Range span = ranges.getSpan();
        float low = span.isLowUnbounded() ? Float.NEGATIVE_INFINITY : realBits(span.getLowValue());
        float high = span.isHighUnbounded() ? Float.POSITIVE_INFINITY : realBits(span.getHighValue());
        if (low == Float.NEGATIVE_INFINITY && high == Float.POSITIVE_INFINITY) {
            return null;
        }
        return new float[]{low, high};
    }

    /** REAL predicate values are the float's int bits widened to long. */
    private static float realBits(Optional<Object> value) {
        return Float.intBitsToFloat(((Long) value.orElseThrow()).intValue());
    }

    // Directional 2-ulp float rounding: outward for the reject box (never drop a true match),
    // inward for the accept box (never accept a true non-match). Two ulps clears the ½-ulp error
    // between a nearest-rounded stored float32 bbox and the true double geometry bound.
    static float outLow(double d)  { return Math.nextDown(Math.nextDown((float) d)); }
    static float outHigh(double d) { return Math.nextUp(Math.nextUp((float) d)); }
    static float inLow(double d)   { return Math.nextUp(Math.nextUp((float) d)); }
    static float inHigh(double d)  { return Math.nextDown(Math.nextDown((float) d)); }
}
