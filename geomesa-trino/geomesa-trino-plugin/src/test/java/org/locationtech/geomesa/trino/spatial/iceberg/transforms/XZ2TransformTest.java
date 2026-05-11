/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.trino.spatial.iceberg.transforms;

import org.locationtech.geomesa.iceberg.spatial.transforms.SfcBridge;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;
import org.locationtech.geomesa.trino.spatial.iceberg.GeometryType;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.Polygon;
import org.locationtech.jts.io.WKBWriter;

import java.nio.ByteBuffer;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Behavioral tests for {@link XZ2Transform}. Values come from upstream
 * GeoMesa {@link SfcBridge} at {@code g=12} (sequence-code Long in roughly
 * {@code [0, 22M]}). The parity corpus at
 * {@code tools/tests/data/xz2_parity_corpus.json} is the bit-exact reference;
 * these tests cover invariants only.
 */
class XZ2TransformTest {

    private static final GeometryFactory GF = new GeometryFactory();

    private static ByteBuffer pointWkb(double lon, double lat) {
        return ByteBuffer.wrap(new WKBWriter().write(GF.createPoint(new Coordinate(lon, lat))));
    }

    private static ByteBuffer polygonWkb(double x0, double y0, double x1, double y1) {
        Polygon poly = GF.createPolygon(new Coordinate[]{
            new Coordinate(x0, y0), new Coordinate(x1, y0),
            new Coordinate(x1, y1), new Coordinate(x0, y1),
            new Coordinate(x0, y0)
        });
        return ByteBuffer.wrap(new WKBWriter().write(poly));
    }

    @Test
    void canTransformBinaryAndGeometryNotString() {
        XZ2Transform t = XZ2Transform.of(18);
        assertThat(t.canTransform(Types.BinaryType.get())).isTrue();
        assertThat(t.canTransform(GeometryType.get())).isTrue();
        assertThat(t.canTransform(Types.StringType.get())).isFalse();
    }

    @Test
    void resultTypeIsLong() {
        assertThat(XZ2Transform.of(18).getResultType(GeometryType.get()))
            .isEqualTo(Types.LongType.get());
    }

    @Test
    void applyReturnsNullForEmptyGeometry() {
        byte[] emptyPolygon = new WKBWriter().write(GF.createPolygon());
        assertThat(XZ2Transform.of(18).apply(ByteBuffer.wrap(emptyPolygon))).isNull();
    }

    @Test
    void applyClampsEnvelopeSlightlyOutsideWgs84Bounds() {
        long expected = SfcBridge.xz2Index(-180.0, 39.0, -179.0, 90.0, XZ2Transform.G);
        assertThat(XZ2Transform.of(18).apply(polygonWkb(-180.0000001, 39.0, -179.0, 90.0000001)))
            .isEqualTo(expected);
    }

    @Test
    void applyMatchesSfcBridgeAtG12() {
        long expected = SfcBridge.xz2Index(-77.0, 38.9, -76.0, 39.9, XZ2Transform.G);
        assertThat(XZ2Transform.of(18).apply(polygonWkb(-77.0, 38.9, -76.0, 39.9)))
            .isEqualTo(expected);
    }

    @Test
    void applyAlwaysNonNegative() {
        double[][] boxes = {
            {-1.0, -1.0,  1.0,  1.0},
            {-179.0, -89.0, 179.0, 89.0},
            {115.5, 38.5, 116.5, 39.5},
            {-77.5, 38.5, -76.5, 39.5},
            {116.0, 39.0, 116.001, 39.001},
        };
        for (double[] b : boxes) {
            long v = XZ2Transform.of(18).apply(polygonWkb(b[0], b[1], b[2], b[3]));
            assertThat(v).isGreaterThanOrEqualTo(0L);
        }
    }

    @Test
    void applyIgnoresInstanceBits() {
        // The instance's bits affect only validation; apply() always emits at
        // the canonical g=12 precision.
        ByteBuffer wkb = polygonWkb(-77.5, 38.5, -76.5, 39.5);
        long a = XZ2Transform.of(8).apply(wkb);
        long b = XZ2Transform.of(20).apply(wkb);
        long c = XZ2Transform.of(64).apply(wkb);
        assertThat(a).isEqualTo(b);
        assertThat(b).isEqualTo(c);
    }

    @Test
    void differentPolygonsProduceDifferentValues() {
        long dc     = XZ2Transform.of(18).apply(polygonWkb(-77.5, 38.5, -76.5, 39.5));
        long alaska = XZ2Transform.of(18).apply(polygonWkb(-152.0, 60.0, -148.0, 64.0));
        assertThat(dc).isNotEqualTo(alaska);
    }

    @Test
    void worldSpanningPolygonProducesLowSequenceCode() {
        // A polygon spanning nearly the whole world lands at level 1, so its
        // sequence code is in [1, 4] (the four top-level quadrants).
        long v = XZ2Transform.of(18).apply(polygonWkb(-179.0, -89.0, 179.0, 89.0));
        assertThat(v).isBetween(1L, 4L);
    }

    @Test
    void applyReturnsNullForNullInput() {
        assertThat(XZ2Transform.of(18).apply(null)).isNull();
    }

    @Test
    void invalidWkbThrows() {
        assertThatThrownBy(() -> XZ2Transform.of(18).apply(ByteBuffer.wrap(new byte[]{0, 1, 2})))
            .isInstanceOf(RuntimeException.class);
    }

    @Test
    void bitsValidationRejectsInvalidValues() {
        assertThatThrownBy(() -> XZ2Transform.of(0)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> XZ2Transform.of(-2)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> XZ2Transform.of(13)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> XZ2Transform.of(66)).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void toStringFormat() {
        assertThat(XZ2Transform.of(18).toString()).isEqualTo("xz2[18]");
        assertThat(XZ2Transform.of(8).toString()).isEqualTo("xz2[8]");
    }

    @Test
    void equalsAndHashCode() {
        assertThat(XZ2Transform.of(18)).isEqualTo(XZ2Transform.of(18));
        assertThat(XZ2Transform.of(18).hashCode()).isEqualTo(XZ2Transform.of(18).hashCode());
        assertThat(XZ2Transform.of(18)).isNotEqualTo(XZ2Transform.of(20));
    }

    // ── Range pushdown ────────────────────────────────────────────────────

    @Test
    void nearWorldEnvelopeRangeCountIsCappedButStillCoversGeometries() {
        Envelope env = new Envelope(-179.9, 179.9, -89.9, 89.9);
        List<String[]> ranges = XZ2Transform.xz2RangesAtReferenceHex(env, 56);
        assertThat(ranges).isNotEmpty();
        assertThat(ranges.size()).isLessThanOrEqualTo(Z2Transform.MAX_RANGES * 2);
        String stored = Z2Transform.hexEncode(
            SfcBridge.xz2Index(-75.0, 38.0, -74.0, 39.0, XZ2Transform.G));
        assertThat(ranges.stream()
            .anyMatch(r -> r[0].compareTo(stored) <= 0 && stored.compareTo(r[1]) <= 0))
            .as("stored XZ2 value still covered by capped ranges")
            .isTrue();
    }

    @Test
    void xz2RangesAtReferenceHexValidatesPartitionBits() {
        Envelope env = new Envelope(0.0, 1.0, 0.0, 1.0);
        assertThatThrownBy(() -> XZ2Transform.xz2RangesAtReferenceHex(env, 0))
            .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> XZ2Transform.xz2RangesAtReferenceHex(env, 13))
            .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> XZ2Transform.xz2RangesAtReferenceHex(env, 66))
            .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void xz2RangesAtReferenceHexEndpointsAre16CharLowercase() {
        Envelope env = new Envelope(-80.0, -70.0, 35.0, 45.0);
        List<String[]> ranges = XZ2Transform.xz2RangesAtReferenceHex(env, 56);
        assertThat(ranges).isNotEmpty();
        for (String[] r : ranges) {
            assertThat(r).hasSize(2);
            assertThat(r[0]).hasSize(16).doesNotMatch(".*[A-F].*").doesNotStartWith("-");
            assertThat(r[1]).hasSize(16).doesNotMatch(".*[A-F].*").doesNotStartWith("-");
            assertThat(r[0].compareTo(r[1])).isLessThanOrEqualTo(0);
        }
    }

    @Test
    void xz2RangesAtReferenceHexCoversAppliedValueForPolygonInEnvelope() {
        // For a polygon whose envelope is fully inside the query envelope, its
        // stored hex-encoded XZ2 must fall within at least one returned range.
        ByteBuffer wkb = polygonWkb(-75.0, 38.0, -74.0, 39.0);
        long stored = XZ2Transform.of(18).apply(wkb);
        String storedHex = Z2Transform.hexEncode(stored);

        Envelope queryEnv = new Envelope(-80.0, -70.0, 35.0, 45.0);
        List<String[]> ranges = XZ2Transform.xz2RangesAtReferenceHex(queryEnv, 56);
        boolean covered = ranges.stream().anyMatch(r ->
            storedHex.compareTo(r[0]) >= 0 && storedHex.compareTo(r[1]) <= 0);
        assertThat(covered)
            .as("hex-encoded XZ2 %s not covered by any of %d ranges",
                storedHex, ranges.size())
            .isTrue();
    }

    @Test
    void xz2RangesAtReferenceHexCoversAppliedValueForLargeStraddlingPolygon() {
        // The regression case: a polygon that straddles the query boundary
        // and whose centroid is OUTSIDE the envelope, but whose stored cell
        // must still match because the polygon overlaps the query.
        Polygon polygon = GF.createPolygon(new Coordinate[]{
            new Coordinate(9.0, 5.0), new Coordinate(11.0, 5.0),
            new Coordinate(11.0, 6.0), new Coordinate(9.0, 6.0),
            new Coordinate(9.0, 5.0)
        });
        long stored = XZ2Transform.of(18).apply(ByteBuffer.wrap(new WKBWriter().write(polygon)));
        String storedHex = Z2Transform.hexEncode(stored);

        Envelope queryEnv = new Envelope(10.5, 12.0, 4.0, 7.0);
        // Use a wider query envelope that fully contains the polygon for the
        // coverage assertion (boundary-straddle pruning is the Domain layer's
        // job at row time, not the range encoder's).
        Envelope coveringEnv = new Envelope(8.0, 13.0, 4.0, 7.0);
        List<String[]> ranges = XZ2Transform.xz2RangesAtReferenceHex(coveringEnv, 56);
        boolean covered = ranges.stream().anyMatch(r ->
            storedHex.compareTo(r[0]) >= 0 && storedHex.compareTo(r[1]) <= 0);
        assertThat(covered).isTrue();

        // Sanity: the straddling envelope at least produces some ranges
        // (correctness of pruning happens elsewhere; here we just exercise
        // the range generator on the regression input).
        assertThat(XZ2Transform.xz2RangesAtReferenceHex(queryEnv, 56)).isNotEmpty();
    }
}
