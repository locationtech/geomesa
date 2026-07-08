/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.trino.spatial.iceberg.transforms;

import org.apache.iceberg.expressions.BoundPredicate;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.expressions.UnboundPredicate;
import org.apache.iceberg.transforms.Transform;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.SerializableFunction;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.Point;
import org.locationtech.jts.io.ParseException;
import org.locationtech.jts.io.WKBReader;

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.List;
import java.util.Objects;

/**
 * Z2 cell encoding via upstream GeoMesa {@code Z2SFC} (31 bits/axis, 62-bit
 * total non-negative Long), <strong>left-shifted by 2</strong> before hex
 * encoding so the top hex char carries 4 bits of real spatial info.
 *
 * <p>Storage column: {@code VARCHAR} holding
 * {@code encodeColumn(Z2SFC.index(lon, lat))} = 16-char unsigned hex of
 * {@code z2 << 2}. The pre-shift Z2SFC value already leaves bits 62 and 63
 * unused (the "MaxMask" reserves them); the shift rotates the useful bits up
 * so {@code truncate(__<X>_z2__, N)} at any {@code N ≥ 1} discriminates by a
 * full 4-bit-wide hex char per width. At {@code N=1} the 16 possible partition
 * values map to the 16 hemispheric quadrants (lat halved twice × lon halved
 * twice). Effective resolution is {@code bits = 4 * N}, range
 * {@code [4, 64]} (the maximum useful Z2SFC information is 62 bits, but the
 * shifted form occupies the full 64-bit hex width).
 *
 * <p>{@code Z2Transform.of(bits)} is preserved for API compatibility with the
 * manifest-clustering tool. {@code bits} is the partition resolution (= 4·width).
 * It does not affect {@link #apply(ByteBuffer)} or {@link #indexOf(double, double)} —
 * Z2SFC always emits at its fixed 62-bit precision.
 */
public class Z2Transform implements Transform<ByteBuffer, Long>, Serializable {

    /** Z2SFC total bits = 62 (31 per axis, GeoMesa default; top 2 bits always zero). */
    public static final int TOTAL_BITS = 62;

    /** Left-shift applied before hex encoding so the lat/lon hemisphere bits
     *  (positions 60-61 of the raw Z2SFC output) land in the top hex char. */
    private static final int SHIFT_BITS = 2;

    /** Low-bit fill on the upper range endpoint after shifting: keeps the range
     *  inclusive of any shifted value whose unshifted form is {@code ≤ hi}. */
    private static final long SHIFT_LOW_MASK = (1L << SHIFT_BITS) - 1;  // = 3

    private static final HexFormat HEX = HexFormat.of();

    /** Rough cap on SFC ranges generated per query envelope, shared by Z2 and XZ2.
     *  Honors GeoMesa's {@code geomesa.scan.ranges.target} system property. Past
     *  the cap the SFC merges adjacent ranges, so the cover stays a superset of the
     *  envelope. Without it, a continent/world-scale envelope can emit tens of
     *  thousands of ranges, bloating the pushed-down Domain built on the coordinator. */
    static final int MAX_RANGES = Integer.getInteger("geomesa.scan.ranges.target", 2000);

    /** Reused per thread — apply() runs per row and WKBReader is not thread-safe. */
    private static final ThreadLocal<WKBReader> WKB_READER =
        ThreadLocal.withInitial(WKBReader::new);

    private final int bits;

    private Z2Transform(int bits) {
        if (bits <= 0 || bits > 64 || bits % 2 != 0) {
            throw new IllegalArgumentException(
                "Z2 bits must be a positive even integer ≤ 64 (got " + bits + ")");
        }
        this.bits = bits;
    }

    public static Z2Transform of(int bits) {
        return new Z2Transform(bits);
    }

    public int bits() {
        return bits;
    }

    @Override
    public SerializableFunction<ByteBuffer, Long> bind(Type type) {
        return this::apply;
    }

    /**
     * Z2SFC index of a Point geometry. Null/empty in -> null out (Iceberg
     * convention). Point-only: centroid-indexing an extended geometry breaks
     * query-side pruning — use {@link XZ2Transform}.
     */
    @Override
    public Long apply(ByteBuffer wkb) {
        if (wkb == null) return null;
        byte[] bytes = new byte[wkb.remaining()];
        wkb.duplicate().get(bytes);
        try {
            Geometry geom = WKB_READER.get().read(bytes);
            if (geom.isEmpty()) return null;
            if (!(geom instanceof Point p)) {
                throw new IllegalArgumentException(
                    "Z2 partitioning requires Point geometries (got " + geom.getGeometryType()
                    + "); use xz2 for extended geometries");
            }
            return SfcBridge.z2Index(p.getX(), p.getY());
        } catch (ParseException e) {
            throw new RuntimeException("Invalid WKB geometry", e);
        }
    }

    /** Returns the Z2SFC index for a single (lon, lat) point. */
    public long indexOf(double lon, double lat) {
        return SfcBridge.z2Index(lon, lat);
    }

    @Override
    public boolean canTransform(Type type) {
        return type.typeId() == Type.TypeID.BINARY;
    }

    @Override
    public Type getResultType(Type sourceType) {
        return Types.LongType.get();
    }

    @Override
    public UnboundPredicate<Long> project(String name, BoundPredicate<ByteBuffer> pred) {
        if (pred.isLiteralPredicate() && pred.op() == Expression.Operation.EQ) {
            ByteBuffer wkb = pred.asLiteralPredicate().literal().value();
            return Expressions.equal(name, apply(wkb));
        }
        return null;
    }

    @Override
    public UnboundPredicate<Long> projectStrict(String name, BoundPredicate<ByteBuffer> pred) {
        return null;
    }

    @Override
    public boolean preservesOrder() {
        return false;
    }

    @Override
    public String toString() {
        return "z2[" + bits + "]";
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) return true;
        if (!(other instanceof Z2Transform)) return false;
        return bits == ((Z2Transform) other).bits;
    }

    @Override
    public int hashCode() {
        return Objects.hash(Z2Transform.class, bits);
    }

    /**
     * Z2 index ranges covering the query envelope, encoded via
     * {@link #encodeColumn(long)} for pushdown against the truncate-partitioned
     * VARCHAR column. Endpoint range becomes {@code [lo << 2, (hi << 2) | 3]} —
     * the OR-3 fill on the upper bound keeps the range inclusive of any
     * shifted value whose unshifted form is {@code ≤ hi} (defensive; shifted
     * stored values always have low 2 bits zero, so the OR is a no-op in
     * practice).
     *
     * @param env           query envelope in WGS84 lon/lat
     * @param partitionBits partition resolution (= 4 * truncate width)
     * @return inclusive {@code [lo, hi]} 16-char lowercase hex ranges
     */
    public static List<String[]> z2RangesAtReferenceHex(Envelope env, int partitionBits) {
        if (partitionBits <= 0 || partitionBits > 64 || (partitionBits & 1) != 0) {
            throw new IllegalArgumentException(
                "partitionBits must be a positive even integer ≤ 64, got " + partitionBits);
        }
        long[][] ranges = SfcBridge.z2RangesAsLongs(
            env.getMinX(), env.getMinY(), env.getMaxX(), env.getMaxY(), MAX_RANGES);
        List<String[]> out = new ArrayList<>(ranges.length);
        for (long[] r : ranges) {
            out.add(new String[] {
                hexEncode(r[0] << SHIFT_BITS),
                hexEncode((r[1] << SHIFT_BITS) | SHIFT_LOW_MASK)
            });
        }
        return out;
    }

    /**
     * Encode a Z2SFC index as the 16-char hex partition value: left-shift by
     * {@link #SHIFT_BITS} (to expose the lat/lon hemisphere bits in the top hex
     * char), then format as unsigned hex. This documents and matches the storage
     * encoding produced by the Python writer; it is exercised by the Java↔Python
     * parity corpus, not by the connector range path (which shifts inline).
     */
    public static String encodeColumn(long z2Index) {
        return hexEncode(z2Index << SHIFT_BITS);
    }

    /**
     * Format a Long as 16-char zero-padded lowercase unsigned hex. Low-level
     * utility used for both Z2 (after shift) and XZ2 (no shift, since XZ2SFC
     * sequence codes don't carry geographic info in their high bits in a
     * way that a fixed bit-shift could exploit).
     */
    public static String hexEncode(long value) {
        return HEX.toHexDigits(value);
    }
}
