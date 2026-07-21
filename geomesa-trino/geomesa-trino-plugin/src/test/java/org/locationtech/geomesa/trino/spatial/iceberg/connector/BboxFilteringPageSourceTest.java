/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.trino.spatial.iceberg.connector;

import io.airlift.slice.Slices;
import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.block.IntArrayBlock;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.SourcePage;
import io.trino.spi.type.VarbinaryType;
import org.junit.jupiter.api.Test;
import org.locationtech.geomesa.trino.spatial.iceberg.connector.BboxFilteringPageSource.BboxBound;
import org.locationtech.geomesa.trino.spatial.iceberg.connector.BboxFilteringPageSource.ShortCircuitConfig;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.io.WKBWriter;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.function.ObjLongConsumer;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Direct behavioural test of {@link BboxFilteringPageSource}: feed a page of bbox sub-field columns
 * (and geometry WKB) through a stub delegate and assert exactly which positions the reject / accept /
 * exact-shell classification keeps. This is the coverage the corpus/parity IT can't give offline —
 * and the guard against a refactor silently turning the filter into a pass-through (every row kept).
 *
 * <p>Geometry subtype is {@code Point}, so each row's bbox is degenerate: {@code xmin==xmax==x} and
 * {@code ymin==ymax==y}. Bounds are built exactly as {@link SpatialPageSourceProvider#planAccept}
 * does, via its directional-rounding helpers, over the query rectangle {@code [0,10] × [0,20]}.
 * Physical channel layout: {@code 0=xmin, 1=ymin, 2=xmax, 3=ymax, 4=geom}.
 */
class BboxFilteringPageSourceTest {

    private static final GeometryFactory GF = new GeometryFactory();
    private static final int XMIN = 0, YMIN = 1, XMAX = 2, YMAX = 3, GEOM = 4;

    /** Reject/accept boxes over [0,10]×[0,20], built the same way the provider builds them. */
    private static ShortCircuitConfig config() {
        double minX = 0, minY = 0, maxX = 10, maxY = 20;
        List<BboxBound> outer = List.of(
            new BboxBound(XMAX, SpatialPageSourceProvider.outLow(minX), Float.POSITIVE_INFINITY),
            new BboxBound(XMIN, Float.NEGATIVE_INFINITY, SpatialPageSourceProvider.outHigh(maxX)),
            new BboxBound(YMAX, SpatialPageSourceProvider.outLow(minY), Float.POSITIVE_INFINITY),
            new BboxBound(YMIN, Float.NEGATIVE_INFINITY, SpatialPageSourceProvider.outHigh(maxY)));
        List<BboxBound> inner = List.of(
            new BboxBound(XMIN, SpatialPageSourceProvider.inLow(minX), Float.POSITIVE_INFINITY),
            new BboxBound(XMAX, Float.NEGATIVE_INFINITY, SpatialPageSourceProvider.inHigh(maxX)),
            new BboxBound(YMIN, SpatialPageSourceProvider.inLow(minY), Float.POSITIVE_INFINITY),
            new BboxBound(YMAX, Float.NEGATIVE_INFINITY, SpatialPageSourceProvider.inHigh(maxY)));
        Geometry rect = GF.createPolygon(new Coordinate[]{
            new Coordinate(minX, minY), new Coordinate(maxX, minY), new Coordinate(maxX, maxY),
            new Coordinate(minX, maxY), new Coordinate(minX, minY)});
        return new ShortCircuitConfig(outer, inner, GEOM, rect);
    }

    /** A page of point rows: bbox leaves from the coordinates, geometry from {@code geoms}
     *  (null ⇒ null WKB). {@code xs.length} must equal {@code ys.length} and {@code geoms.length}. */
    private static SourcePage pointPage(float[] xs, float[] ys, Geometry[] geoms) {
        return new StubSourcePage(xs.length,
            realBlock(xs), realBlock(ys), realBlock(xs), realBlock(ys), geomBlock(geoms));
    }

    /** A page whose bbox leaves are all SQL NULL (forcing the exact-shell path), with the given geoms. */
    private static SourcePage nullBboxPage(Geometry[] geoms) {
        Block nulls = nullRealBlock(geoms.length);
        return new StubSourcePage(geoms.length, nulls, nulls, nulls, nulls, geomBlock(geoms));
    }

    private static int filteredPositions(SourcePage page, ShortCircuitConfig config,
                                         int outputChannelCount, boolean strip) {
        var src = new BboxFilteringPageSource(new OneShotPageSource(page), outputChannelCount, strip, config);
        SourcePage out = src.getNextSourcePage();
        return out.getPositionCount();
    }

    @Test
    void rejectsPointsWhollyOutsideEnvelope() {
        // (-5,10) left of box, (50,10) right, (5,30) above — none can intersect [0,10]×[0,20].
        SourcePage page = pointPage(new float[]{-5, 50, 5}, new float[]{10, 10, 30},
            new Geometry[]{point(-5, 10), point(50, 10), point(5, 30)});
        assertThat(filteredPositions(page, config(), 5, false)).isEqualTo(0);
    }

    @Test
    void acceptsPointsWhollyInsideEnvelopeWithoutDecodingGeometry() {
        // All strictly inside the inward-rounded accept box. Geometry blocks are NULL: if the accept
        // path wrongly fell through to the exact test it would decode null and drop them → 0, failing.
        SourcePage page = pointPage(new float[]{5, 2, 8}, new float[]{10, 5, 15},
            new Geometry[]{null, null, null});
        assertThat(filteredPositions(page, config(), 5, false)).isEqualTo(3);
    }

    @Test
    void keepsOnlyIntersectingPointsInMixedPage() {
        // in, out, in, out, in  →  3 kept. The regression guard: a pass-through filter returns 5.
        SourcePage page = pointPage(
            new float[]{5, -5, 2, 50, 8},
            new float[]{10, 10, 5, 10, 15},
            new Geometry[]{point(5, 10), point(-5, 10), point(2, 5), point(50, 10), point(8, 15)});
        assertThat(filteredPositions(page, config(), 5, false)).isEqualTo(3);
    }

    @Test
    void countStarConfigurationStripsChannelsAndReportsFilteredCount() {
        // Mirrors the production count(*) path: no query columns, so outputChannelCount = 0 and the
        // added bbox/geom channels are stripped. The reported position count must be the FILTERED
        // count, not the raw input — this is the exact shape the flag-on EXPLAIN showed passing 4.44B.
        SourcePage page = pointPage(
            new float[]{5, -5, 2, 50, 8},
            new float[]{10, 10, 5, 10, 15},
            new Geometry[]{point(5, 10), point(-5, 10), point(2, 5), point(50, 10), point(8, 15)});
        var src = new BboxFilteringPageSource(new OneShotPageSource(page), 0, true, config());
        SourcePage out = src.getNextSourcePage();
        assertThat(out.getPositionCount()).as("filtered position count").isEqualTo(3);
        assertThat(out.getChannelCount()).as("added channels hidden from engine").isEqualTo(0);
    }

    @Test
    void nullBboxRowFallsThroughToExactGeometryTest() {
        // Null bbox can prove neither reject nor accept, so each row decodes its WKB and runs the
        // exact intersection: the (5,10) point matches, the (50,10) point does not.
        SourcePage page = nullBboxPage(new Geometry[]{point(5, 10), point(50, 10)});
        assertThat(filteredPositions(page, config(), 5, false)).isEqualTo(1);
    }

    // ---- fixtures ---------------------------------------------------------------------------------

    private static Geometry point(double x, double y) {
        return GF.createPoint(new Coordinate(x, y));
    }

    /** REAL block (float32) from the given values, no nulls. */
    private static Block realBlock(float... values) {
        int[] bits = new int[values.length];
        for (int i = 0; i < values.length; i++) {
            bits[i] = Float.floatToIntBits(values[i]);
        }
        return new IntArrayBlock(values.length, Optional.empty(), bits);
    }

    /** REAL block whose every position is SQL NULL. */
    private static Block nullRealBlock(int positions) {
        boolean[] isNull = new boolean[positions];
        Arrays.fill(isNull, true);
        return new IntArrayBlock(positions, Optional.of(isNull), new int[positions]);
    }

    /** VARBINARY block of WKB-encoded geometries (null ⇒ SQL NULL). */
    private static Block geomBlock(Geometry[] geoms) {
        WKBWriter writer = new WKBWriter();
        BlockBuilder builder = VarbinaryType.VARBINARY.createBlockBuilder(null, geoms.length);
        for (Geometry g : geoms) {
            if (g == null) {
                builder.appendNull();
            } else {
                VarbinaryType.VARBINARY.writeSlice(builder, Slices.wrappedBuffer(writer.write(g)));
            }
        }
        return builder.build();
    }

    /** Minimal in-memory {@link SourcePage}; {@link #selectPositions} narrows it in place so the
     *  post-filter {@link #getPositionCount()} reflects what the page source retained. */
    private static final class StubSourcePage implements SourcePage {
        private Block[] blocks;
        private int positionCount;

        StubSourcePage(int positionCount, Block... blocks) {
            this.positionCount = positionCount;
            this.blocks = blocks;
        }

        @Override public int getPositionCount() { return positionCount; }
        @Override public long getSizeInBytes() { return 0; }
        @Override public long getRetainedSizeInBytes() { return 0; }
        @Override public void retainedBytesForEachPart(ObjLongConsumer<Object> consumer) {}
        @Override public int getChannelCount() { return blocks.length; }
        @Override public Block getBlock(int channel) { return blocks[channel]; }
        @Override public Page getPage() { return new Page(positionCount, blocks); }

        @Override
        public void selectPositions(int[] positions, int offset, int size) {
            Block[] narrowed = new Block[blocks.length];
            for (int i = 0; i < blocks.length; i++) {
                narrowed[i] = blocks[i].getPositions(positions, offset, size);
            }
            blocks = narrowed;
            positionCount = size;
        }
    }

    /** Delegate that yields one page then signals exhaustion. */
    private static final class OneShotPageSource implements ConnectorPageSource {
        private SourcePage page;

        OneShotPageSource(SourcePage page) { this.page = page; }

        @Override public SourcePage getNextSourcePage() {
            SourcePage next = page;
            page = null;
            return next;
        }

        @Override public boolean isFinished() { return page == null; }
        @Override public long getCompletedBytes() { return 0; }
        @Override public long getReadTimeNanos() { return 0; }
        @Override public long getMemoryUsage() { return 0; }
        @Override public OptionalLong getCompletedPositions() { return OptionalLong.empty(); }
        @Override public void close() {}
    }
}
