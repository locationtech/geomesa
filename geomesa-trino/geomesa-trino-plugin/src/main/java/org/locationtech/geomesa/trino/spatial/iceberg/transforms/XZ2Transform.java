/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.trino.spatial.iceberg.transforms;

import org.apache.iceberg.expressions.BoundPredicate;
import org.apache.iceberg.expressions.UnboundPredicate;
import org.apache.iceberg.transforms.Transform;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.SerializableFunction;
import org.locationtech.geomesa.iceberg.spatial.transforms.SfcBridge;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.io.ParseException;
import org.locationtech.jts.io.WKBReader;

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * XZ2 cell encoding via upstream GeoMesa {@code XZ2SFC} at {@code g=12}
 * (sequence-code Long in roughly {@code [0, 22M]}), formatted as 16-char
 * zero-padded lowercase unsigned hex.
 *
 * <p>XZ2SFC sequence codes at g=12 are ≤ 2^25, so every stored value shares
 * the leading 8 hex chars {@code "00000000"}. Useful spatial discrimination
 * on the truncate-partitioned column therefore starts at width ≥ 13 —
 * narrower widths produce a single partition for every row.
 *
 * <p>{@code XZ2Transform.of(bits)} is preserved for API parity with
 * {@link Z2Transform}. {@code bits} is the partition resolution (= 4·width)
 * and does not affect {@link #apply(ByteBuffer)} — XZ2SFC(g=12) always emits
 * at its fixed sequence-code precision.
 */
public class XZ2Transform implements Transform<ByteBuffer, Long>, Serializable {

    /** XZ2 quad-tree depth. Cloud writer uses g=12. */
    public static final short G = 12;

    /** Reused per thread — apply() runs per row and WKBReader is not thread-safe. */
    private static final ThreadLocal<WKBReader> WKB_READER =
        ThreadLocal.withInitial(WKBReader::new);

    private final int bits;

    private XZ2Transform(int bits) {
        if (bits <= 0 || bits > 64 || bits % 2 != 0) {
            throw new IllegalArgumentException(
                "XZ2 bits must be a positive even integer ≤ 64 (got " + bits + ")");
        }
        this.bits = bits;
    }

    public static XZ2Transform of(int bits) {
        return new XZ2Transform(bits);
    }

    public int bits() {
        return bits;
    }

    /**
     * Returns the XZ2SFC sequence code for the geometry's envelope at g=12.
     * Null/empty in -> null out (Iceberg convention).
     */
    @Override
    public Long apply(ByteBuffer wkb) {
        if (wkb == null) return null;
        byte[] bytes = new byte[wkb.remaining()];
        wkb.duplicate().get(bytes);
        try {
            Geometry geom = WKB_READER.get().read(bytes);
            if (geom.isEmpty()) return null;
            Envelope env = geom.getEnvelopeInternal();
            return SfcBridge.xz2Index(
                env.getMinX(), env.getMinY(), env.getMaxX(), env.getMaxY(), G);
        } catch (ParseException e) {
            throw new RuntimeException("Invalid WKB geometry", e);
        }
    }

    /**
     * XZ2 index ranges covering the query envelope at g=12, hex-encoded for
     * pushdown against the truncate-partitioned VARCHAR column.
     * {@link SfcBridge#xz2RangesAsLongs} returns non-negative Longs, so the
     * unsigned-hex endpoints are monotonic in byte-lex order.
     *
     * @param env           query envelope in WGS84 lon/lat
     * @param partitionBits partition resolution (= 4 * truncate width). Widths
     *                      below 13 do not discriminate spatially (all stored
     *                      values share the leading 8 hex chars), but the
     *                      Domain is still emitted for correctness.
     * @return inclusive {@code [lo, hi]} 16-char lowercase hex ranges
     */
    public static List<String[]> xz2RangesAtReferenceHex(Envelope env, int partitionBits) {
        if (partitionBits <= 0 || partitionBits > 64 || (partitionBits & 1) != 0) {
            throw new IllegalArgumentException(
                "partitionBits must be a positive even integer ≤ 64, got " + partitionBits);
        }
        long[][] ranges = SfcBridge.xz2RangesAsLongs(
            env.getMinX(), env.getMinY(), env.getMaxX(), env.getMaxY(), G, Z2Transform.MAX_RANGES);
        List<String[]> out = new ArrayList<>(ranges.length);
        for (long[] r : ranges) {
            out.add(new String[] { Z2Transform.hexEncode(r[0]), Z2Transform.hexEncode(r[1]) });
        }
        return out;
    }

    @Override
    public SerializableFunction<ByteBuffer, Long> bind(Type type) {
        return this::apply;
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
    public boolean preservesOrder() {
        return false;
    }

    @Override
    public UnboundPredicate<Long> project(String name, BoundPredicate<ByteBuffer> pred) {
        return null;
    }

    @Override
    public UnboundPredicate<Long> projectStrict(String name, BoundPredicate<ByteBuffer> pred) {
        return null;
    }

    @Override
    public String toString() {
        return "xz2[" + bits + "]";
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) return true;
        if (!(other instanceof XZ2Transform)) return false;
        return bits == ((XZ2Transform) other).bits;
    }

    @Override
    public int hashCode() {
        return Objects.hash(XZ2Transform.class, bits);
    }
}
