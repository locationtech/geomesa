/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.trino.spatial.iceberg.connector;

import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.ConnectorPageSourceProvider;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.connector.ConnectorTableCredentials;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.DynamicFilter;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import org.locationtech.geomesa.trino.spatial.iceberg.connector.BboxFilteringPageSource.BboxBound;
import org.locationtech.geomesa.trino.spatial.iceberg.connector.BboxFilteringPageSource.ShortCircuitConfig;

import static org.locationtech.geomesa.trino.spatial.iceberg.connector.SpatialTableHandle.XMAX;
import static org.locationtech.geomesa.trino.spatial.iceberg.connector.SpatialTableHandle.XMIN;
import static org.locationtech.geomesa.trino.spatial.iceberg.connector.SpatialTableHandle.YMAX;
import static org.locationtech.geomesa.trino.spatial.iceberg.connector.SpatialTableHandle.YMIN;

/**
 * Wraps the Iceberg page-source provider to apply the connector's BBOX short-circuit in
 * {@link BboxFilteringPageSource}: when the handle is a {@link SpatialTableHandle} (the connector
 * claimed a rectangle {@code ST_Intersects} on a point column as enforced), this reads the bbox
 * sub-fields + geometry, builds outer (reject) and inner (accept) boxes from the exact rectangle,
 * and does the authoritative reject/accept/exact filtering.
 *
 * <p>For any other handle it delegates unchanged — zero overhead. The handle is always unwrapped to
 * its underlying Iceberg table handle before the delegate reads it; a missed unwrap would surface as
 * a loud {@code ClassCastException}, never a wrong result.
 */
final class SpatialPageSourceProvider implements ConnectorPageSourceProvider {

    private static final Logger LOG = LoggerFactory.getLogger(SpatialPageSourceProvider.class);
    private static final GeometryFactory GEOMETRY_FACTORY = new GeometryFactory();

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

    /** The physical columns to read (query columns, plus bbox sub-fields — and, in bbox-short-circuit
     *  mode, the geometry — appended to evaluate the filter) and how to wrap the page source. */
    private record Plan(List<ColumnHandle> columns, int outputChannelCount,
                        boolean stripAddedChannels, ShortCircuitConfig shortCircuitConfig) {
        ConnectorPageSource wrap(ConnectorPageSource ps) {
            return shortCircuitConfig == null ? ps
                : new BboxFilteringPageSource(ps, outputChannelCount, stripAddedChannels, shortCircuitConfig);
        }
    }

    private Plan plan(ConnectorTableHandle table, List<ColumnHandle> columns) {
        if (table instanceof SpatialTableHandle sth) {
            LOG.debug("bbox short-circuit ENGAGED (page-source filtering active)");
            return planAccept(sth, columns);
        }
        // If a rectangle ST_Intersects on points was claimed enforced but the handle arrives here as a
        // plain Iceberg handle, the filter silently no-ops and the engine sees every row — log which.
        LOG.debug("bbox short-circuit NOT engaged; passthrough for handle {}", table.getClass().getName());
        return new Plan(columns, columns.size(), false, null);
    }

    /** Bbox-short-circuit: authoritative reject/accept/exact. Outer (reject) and inner (accept) boxes are
     *  built from the exact query rectangle with directional 2-ulp float rounding so a row that the
     *  nearest-rounded stored bbox could misclassify always falls through to the exact test. */
    private Plan planAccept(SpatialTableHandle tableHandle, List<ColumnHandle> columns) {
        List<ColumnHandle> augmented = new ArrayList<>(columns);
        int[] channels = new int[4];
        for (int i = 0; i < 4; i++) {
            channels[i] = channelFor(augmented, tableHandle.bboxLeaves().get(i));
        }
        int geomCh = channelFor(augmented, tableHandle.geomColumn());

        double minX = tableHandle.rectMinX(),
               minY = tableHandle.rectMinY(),
               maxX = tableHandle.rectMaxX(),
               maxY = tableHandle.rectMaxY();

        // Reject box (necessary overlap condition): expanded outward.
        List<BboxBound> outer = List.of(
            new BboxBound(channels[XMAX], outLow(minX), Float.POSITIVE_INFINITY),
            new BboxBound(channels[XMIN], Float.NEGATIVE_INFINITY, outHigh(maxX)),
            new BboxBound(channels[YMAX], outLow(minY), Float.POSITIVE_INFINITY),
            new BboxBound(channels[YMIN], Float.NEGATIVE_INFINITY, outHigh(maxY)));
        // Accept box (sufficient containment condition): shrunk inward — no WKB decode needed.
        List<BboxBound> inner = List.of(
            new BboxBound(channels[XMIN], inLow(minX), Float.POSITIVE_INFINITY),
            new BboxBound(channels[XMAX], Float.NEGATIVE_INFINITY, inHigh(maxX)),
            new BboxBound(channels[YMIN], inLow(minY), Float.POSITIVE_INFINITY),
            new BboxBound(channels[YMAX], Float.NEGATIVE_INFINITY, inHigh(maxY)));

        Geometry queryRect = GEOMETRY_FACTORY.createPolygon(new Coordinate[]{
            new Coordinate(minX, minY), new Coordinate(maxX, minY), new Coordinate(maxX, maxY),
            new Coordinate(minX, maxY), new Coordinate(minX, minY)});

        boolean stripped = augmented.size() > columns.size();
        return new Plan(augmented, columns.size(), stripped,
            new ShortCircuitConfig(outer, inner, geomCh, queryRect));
    }

    private static int channelFor(List<ColumnHandle> columns, ColumnHandle handle) {
        int i = columns.indexOf(handle);
        if (i >= 0) {
            return i;
        }
        columns.add(handle);
        return columns.size() - 1;
    }

    // Directional 2-ulp float rounding: outward for the reject box (never drop a true match),
    // inward for the accept box (never accept a true non-match). Two ulps clears the ½-ulp error
    // between a nearest-rounded stored float32 bbox and the true double geometry bound.
    static float outLow(double d)  { return Math.nextDown(Math.nextDown((float) d)); }
    static float outHigh(double d) { return Math.nextUp(Math.nextUp((float) d)); }
    static float inLow(double d)   { return Math.nextUp(Math.nextUp((float) d)); }
    static float inHigh(double d)  { return Math.nextDown(Math.nextDown((float) d)); }
}
