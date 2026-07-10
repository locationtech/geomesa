/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.trino.spatial.iceberg.transforms;

import org.locationtech.jts.geom.Envelope;

import java.util.Arrays;
import java.util.List;

/**
 * Hex-encoded Z2/XZ2 index ranges covering a query envelope, for pushdown
 * against hex-partitioned storage columns ({@code __<X>_z2__} /
 * {@code __<X>_xz2__}).
 *
 * <p>Endpoints are encoded with {@code Z2SFC.hexEncode}/{@code XZ2SFC.hexEncode}
 * (via {@link SfcBridge}), which left-align the significant bits so
 * lexicographic comparison over the fixed-width strings matches numeric
 * comparison of the underlying index values. That makes the inclusive
 * {@code [lo, hi]} string ranges directly usable both as VARCHAR domains on
 * the full column value and — because prefix order is preserved — under
 * Iceberg {@code truncate(width)} partition projection at any width.
 */
public final class SpatialIndexRanges {

    /** XZ2 quad-tree depth; matches the GeoMesa writer default precision. */
    public static final short G = 12;

    /** Rough cap on SFC ranges generated per query envelope, shared by Z2 and XZ2.
     *  Honors GeoMesa's {@code geomesa.scan.ranges.target} system property. Past
     *  the cap the SFC merges adjacent ranges, so the cover stays a superset of the
     *  envelope. Without it, a continent/world-scale envelope can emit tens of
     *  thousands of ranges, bloating the pushed-down Domain built on the coordinator. */
    static final int MAX_RANGES = Integer.getInteger("geomesa.scan.ranges.target", 2000);

    private SpatialIndexRanges() {}

    /**
     * Z2 index ranges covering the query envelope, hex-encoded for pushdown
     * against a {@code __<X>_z2__} column.
     *
     * @param env query envelope in WGS84 lon/lat
     * @return inclusive {@code [lo, hi]} hex ranges
     */
    public static List<String[]> z2Ranges(Envelope env) {
        return Arrays.asList(SfcBridge.z2HexRanges(
            env.getMinX(), env.getMinY(), env.getMaxX(), env.getMaxY(), MAX_RANGES));
    }

    /**
     * XZ2 index ranges covering the query envelope at {@link #G}, hex-encoded
     * for pushdown against a {@code __<X>_xz2__} column.
     *
     * @param env query envelope in WGS84 lon/lat
     * @return inclusive {@code [lo, hi]} hex ranges
     */
    public static List<String[]> xz2Ranges(Envelope env) {
        return Arrays.asList(SfcBridge.xz2HexRanges(
            env.getMinX(), env.getMinY(), env.getMaxX(), env.getMaxY(), G, MAX_RANGES));
    }
}
