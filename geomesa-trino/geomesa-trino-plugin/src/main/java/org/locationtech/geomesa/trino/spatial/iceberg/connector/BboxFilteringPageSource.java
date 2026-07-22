/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.trino.spatial.iceberg.connector;

import io.airlift.slice.Slice;
import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.SourcePage;
import io.trino.spi.metrics.Metrics;
import io.trino.spi.type.RealType;
import io.trino.spi.type.VarbinaryType;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.io.WKBReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.OptionalLong;
import java.util.concurrent.CompletableFuture;
import java.util.function.ObjLongConsumer;

/**
 * Wraps the Iceberg page source with a sound, pre-decode bbox filter over a geometry's
 * {@code __X_bbox__} sub-fields, which the connector injected as REAL domains and which ride to the
 * worker in the table handle's unenforced predicate. Two modes, selected by whether an
 * {@link AcceptConfig} is supplied:
 *
 * <ul>
 *   <li><strong>Reject-only</strong> ({@code accept == null}) — drops rows whose bbox cannot overlap
 *       the query envelope and hands the survivors to the engine's exact {@code ST_*} residual. A
 *       sound pre-filter for any spatial predicate the connector could not claim enforced
 *       (non-rectangle query, non-point column, {@code ST_Contains}, …): it saves the WKB decode on
 *       the rejected rows without changing the result.</li>
 *   <li><strong>Short-circuit</strong> ({@code accept != null}) — the connector claimed a rectangle
 *       {@code ST_Intersects} on a point column enforced, so this is the authoritative filter:
 *       <ul>
 *         <li>bbox outside the reject box (float32-outward-rounded) ⇒ <strong>reject</strong>;</li>
 *         <li>bbox inside the accept box (float32-inward-rounded) ⇒ <strong>accept</strong> with no
 *             WKB decode — the geometry is provably inside the query rectangle;</li>
 *         <li>otherwise (thin boundary shell, or a null bbox) ⇒ decode the WKB and run the
 *             <strong>exact</strong> {@code intersects} test.</li>
 *       </ul>
 *       The outward/inward rounding guarantees any row a nearest-rounded float32 bbox could
 *       misclassify falls through to the exact test, so the result equals the engine's exact
 *       predicate.</li>
 * </ul>
 *
 * <p>Both modes share the same reject pass over {@link #rejectBounds}; short-circuit adds the
 * accept/shell classification on the survivors.
 *
 * <p><strong>Laziness.</strong> Only the cheap bbox columns are materialized to classify rows; the
 * geometry column is fetched only when a shell row appears in a page, and decoded only at the shell
 * positions. Accept and reject rows never touch the WKB. Any bbox/geometry columns this page source
 * added to the physical read (beyond what the query projected) are stripped from the returned page.
 */
final class BboxFilteringPageSource implements ConnectorPageSource {

    private static final Logger LOG = LoggerFactory.getLogger(BboxFilteringPageSource.class);

    /** One bbox-box bound: the row's real value in {@code channel} must lie within {@code [low, high]}. */
    record BboxBound(int channel, float low, float high) {}

    /** Short-circuit accept configuration; {@code null} ⇒ reject-only mode. {@code innerBounds}
     *  are the containment bounds (ALL must hold ⇒ accept with no WKB decode); {@code geomChannel} is
     *  the physical channel of the geometry WKB column; {@code queryRect} is the exact query rectangle
     *  for the boundary-shell test. */
    record AcceptConfig(List<BboxBound> innerBounds, int geomChannel, Geometry queryRect) {}

    private final ConnectorPageSource delegate;
    /** Reject box: a row whose non-null bbox violates any bound is provably outside the envelope. */
    private final List<BboxBound> rejectBounds;
    /** Accept/exact config in short-circuit mode; {@code null} ⇒ reject-only pre-filter. */
    private final AcceptConfig accept;

    /** Number of channels the query actually requested; any beyond this were added to read the
     *  bbox sub-fields (and, in short-circuit mode, the geometry) and must be hidden from the engine. */
    private final int outputChannelCount;
    private final boolean stripAddedChannels;

    BboxFilteringPageSource(ConnectorPageSource delegate,
                            int outputChannelCount,
                            boolean stripAddedChannels,
                            List<BboxBound> rejectBounds,
                            AcceptConfig accept) {
        this.delegate = delegate;
        this.outputChannelCount = outputChannelCount;
        this.stripAddedChannels = stripAddedChannels;
        this.rejectBounds = rejectBounds;
        this.accept = accept;
    }

    @Override
    public SourcePage getNextSourcePage() {
        SourcePage page = delegate.getNextSourcePage();
        if (page == null) {
            return null;
        }
        int positions = page.getPositionCount();

        Block[] rejectBlocks = materialize(page, rejectBounds);
        Block[] innerBlocks = accept == null ? null : materialize(page, accept.innerBounds());
        Block geomBlock = null;   // geometry column + reader fetched lazily, only if a shell row appears
        WKBReader reader = null;

        int[] retained = new int[positions];
        int kept = 0;
        for (int p = 0; p < positions; p++) {
            if (rejected(p, rejectBlocks, rejectBounds)) {
                continue;   // provably outside the envelope → drop
            }
            if (accept == null) {
                retained[kept++] = p;
                continue;   // reject-only: the engine's exact ST_ still runs on the survivors
            }
            if (accepted(p, innerBlocks, accept.innerBounds())) {
                retained[kept++] = p;
                continue;   // provably inside the envelope → accept with no WKB decode
            }
            // Boundary shell (or null bbox): decode WKB and run the exact test.
            if (geomBlock == null) {
                geomBlock = page.getBlock(accept.geomChannel());
                reader = new WKBReader();
            }
            if (shellMatches(geomBlock, p, reader, accept.queryRect())) {
                retained[kept++] = p;
            }
        }

        if (kept < positions) {
            page.selectPositions(retained, 0, kept);
        }
        return stripAddedChannels ? new ChannelPrefixSourcePage(page, outputChannelCount) : page;
    }

    private static Block[] materialize(SourcePage page, List<BboxBound> bounds) {
        Block[] blocks = new Block[bounds.size()];
        for (int i = 0; i < bounds.size(); i++) {
            blocks[i] = page.getBlock(bounds.get(i).channel());
        }
        return blocks;
    }

    /** True iff the row is provably outside the query envelope — any outer bound is violated by a
     *  non-null bbox value. A null bbox value cannot prove non-overlap, so it never rejects. */
    private static boolean rejected(int position, Block[] blocks, List<BboxBound> bounds) {
        for (int i = 0; i < bounds.size(); i++) {
            Block b = blocks[i];
            if (b.isNull(position)) {
                continue;
            }
            float v = RealType.REAL.getFloat(b, position);
            BboxBound bound = bounds.get(i);
            if (v < bound.low() || v > bound.high()) {
                return true;
            }
        }
        return false;
    }

    /** True iff the row's bbox is provably inside the (inward-rounded) query rectangle — every inner
     *  containment bound holds. A null bbox value cannot prove containment, so it is NOT accepted
     *  (it falls through to the exact test). */
    private static boolean accepted(int position, Block[] blocks, List<BboxBound> bounds) {
        for (int i = 0; i < bounds.size(); i++) {
            Block b = blocks[i];
            if (b.isNull(position)) {
                return false;
            }
            float v = RealType.REAL.getFloat(b, position);
            BboxBound bound = bounds.get(i);
            if (v < bound.low() || v > bound.high()) {
                return false;
            }
        }
        return true;
    }

    /** Exact test for a boundary-shell row: decode the geometry WKB and test intersection against
     *  the query rectangle. A null or undecodable geometry cannot match, so it is dropped (this
     *  mirrors the engine excluding a row whose {@code ST_GeomFromBinary} yields null). */
    private boolean shellMatches(Block geomBlock, int position, WKBReader reader, Geometry queryRect) {
        if (geomBlock.isNull(position)) {
            return false;
        }
        Slice slice = VarbinaryType.VARBINARY.getSlice(geomBlock, position);
        try {
            Geometry g = reader.read(slice.getBytes());
            return g != null && !g.isEmpty() && g.intersects(queryRect);
        } catch (Exception e) {
            LOG.warn("Dropping row with undecodable geometry WKB during bbox bbox-short-circuit: {}", e.toString());
            return false;
        }
    }

    /**
     * Pure delegation
     */
    @Override public long getCompletedBytes() { return delegate.getCompletedBytes(); }
    @Override public OptionalLong getCompletedPositions() { return delegate.getCompletedPositions(); }
    @Override public long getReadTimeNanos() { return delegate.getReadTimeNanos(); }
    @Override public boolean isFinished() { return delegate.isFinished(); }
    @Override public long getMemoryUsage() { return delegate.getMemoryUsage(); }
    @Override public void close() throws IOException { delegate.close(); }
    @Override public CompletableFuture<?> isBlocked() { return delegate.isBlocked(); }
    @Override public Metrics getMetrics() { return delegate.getMetrics(); }

    /** A {@link SourcePage} view exposing only the first {@code channelCount} channels of a
     *  delegate, hiding the bbox/geometry columns this page source added to the physical read. */
    private static final class ChannelPrefixSourcePage implements SourcePage {
        private final SourcePage delegate;
        private final int channelCount;

        ChannelPrefixSourcePage(SourcePage delegate, int channelCount) {
            this.delegate = delegate;
            this.channelCount = channelCount;
        }

        @Override public int getPositionCount() { return delegate.getPositionCount(); }
        @Override public long getSizeInBytes() { return delegate.getSizeInBytes(); }
        @Override public long getRetainedSizeInBytes() { return delegate.getRetainedSizeInBytes(); }
        @Override public void retainedBytesForEachPart(ObjLongConsumer<Object> consumer) {
            delegate.retainedBytesForEachPart(consumer);
        }
        @Override public int getChannelCount() { return channelCount; }
        @Override public Block getBlock(int channel) {
            if (channel >= channelCount) {
                throw new IndexOutOfBoundsException("channel " + channel + " >= " + channelCount);
            }
            return delegate.getBlock(channel);
        }
        @Override public Page getPage() {
            int[] channels = new int[channelCount];
            for (int i = 0; i < channelCount; i++) {
                channels[i] = i;
            }
            return delegate.getColumns(channels);
        }
        @Override public Page getColumns(int[] channels) { return delegate.getColumns(channels); }
        @Override public void selectPositions(int[] positions, int offset, int size) {
            delegate.selectPositions(positions, offset, size);
        }
    }
}
