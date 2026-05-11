/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.trino.spatial.iceberg.transforms;

import org.locationtech.geomesa.iceberg.spatial.transforms.SfcBridge;
import org.junit.jupiter.api.Test;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.Point;
import org.locationtech.jts.io.WKBWriter;

import java.nio.ByteBuffer;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * End-to-end spatial-correctness tests for the Z2/XZ2 range-pushdown path.
 * Asserts that for arbitrary points sampled from a query envelope, the
 * stored lex-encoded SFC value lands inside at least one of the returned
 * ranges — the core pushdown-correctness invariant.
 */
class Z2SpatialTest {

    private static final GeometryFactory GF = new GeometryFactory();

    private static ByteBuffer wkb(double lon, double lat) {
        Point p = GF.createPoint(new Coordinate(lon, lat));
        return ByteBuffer.wrap(new WKBWriter().write(p));
    }

    @Test
    void z2RangesCoverEveryAppliedPointInEnvelope() {
        Envelope env = new Envelope(-90.0, 90.0, -45.0, 45.0);
        List<String[]> ranges = Z2Transform.z2RangesAtReferenceHex(env, 20);
        double lonStep = (env.getMaxX() - env.getMinX()) / 8.0;
        double latStep = (env.getMaxY() - env.getMinY()) / 8.0;
        for (int i = 0; i < 8; i++) {
            for (int j = 0; j < 8; j++) {
                double lon = env.getMinX() + i * lonStep + lonStep / 2.0;
                double lat = env.getMinY() + j * latStep + latStep / 2.0;
                String stored = Z2Transform.encodeColumn(SfcBridge.z2Index(lon, lat));
                boolean covered = ranges.stream().anyMatch(r ->
                    stored.compareTo(r[0]) >= 0 && stored.compareTo(r[1]) <= 0);
                assertThat(covered)
                    .as("Point (%s, %s) hex %s not covered by any of %d ranges",
                        lon, lat, stored, ranges.size())
                    .isTrue();
            }
        }
    }

    @Test
    void z2RangesCoverEnvelopeCorners() {
        Envelope env = new Envelope(-80.0, -70.0, 37.0, 47.0);
        List<String[]> ranges = Z2Transform.z2RangesAtReferenceHex(env, 20);
        for (double lon : new double[]{env.getMinX(), env.getMaxX()}) {
            for (double lat : new double[]{env.getMinY(), env.getMaxY()}) {
                String stored = Z2Transform.encodeColumn(SfcBridge.z2Index(lon, lat));
                boolean covered = ranges.stream().anyMatch(r ->
                    stored.compareTo(r[0]) >= 0 && stored.compareTo(r[1]) <= 0);
                assertThat(covered).as("Corner (%s, %s)", lon, lat).isTrue();
            }
        }
    }

    @Test
    void z2RangesNonEmptyForFullGlobeEnvelope() {
        Envelope env = new Envelope(-180.0, 180.0, -90.0, 90.0);
        assertThat(Z2Transform.z2RangesAtReferenceHex(env, 20)).isNotEmpty();
    }

    @Test
    void z2RangesNonEmptyForPointEnvelope() {
        Envelope env = new Envelope(-77.0, -77.0, 38.9, 38.9);
        List<String[]> ranges = Z2Transform.z2RangesAtReferenceHex(env, 20);
        assertThat(ranges).isNotEmpty();
        String stored = Z2Transform.encodeColumn(SfcBridge.z2Index(-77.0, 38.9));
        boolean covered = ranges.stream().anyMatch(r ->
            stored.compareTo(r[0]) >= 0 && stored.compareTo(r[1]) <= 0);
        assertThat(covered).isTrue();
    }

    @Test
    void nullWkbPassthrough() {
        assertThat(Z2Transform.of(18).apply(null)).isNull();
    }
}
