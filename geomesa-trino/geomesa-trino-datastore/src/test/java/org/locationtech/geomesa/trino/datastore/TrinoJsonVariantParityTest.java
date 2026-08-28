/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.trino.datastore;

import org.apache.parquet.variant.Variant;
import org.apache.parquet.variant.VariantBuilder;
import org.apache.parquet.variant.VariantObjectBuilder;
import org.junit.jupiter.api.Test;
import org.locationtech.geomesa.fs.storage.core.parquet.io.VariantJsonWriter;

import java.io.IOException;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.Consumer;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The two DataStores that can serve the same document have to render it identically.
 *
 * <p>A GeoMesa {@code json=true} attribute is stored as a Parquet {@code variant}. Read through the
 * FileSystem DataStore, {@code VariantJsonWriter} renders that variant. Read through this DataStore,
 * Trino decodes the same variant and {@link TrinoJson} renders the decoded objects. A consumer that
 * compares the two — an FSDS export against a Trino export, a cached document against a re-read one —
 * sees a difference for any leaf the two disagree on, so each case below renders one leaf both ways
 * and asserts the strings are equal rather than merely both plausible.
 *
 * <p>The Java object each case passes to {@link TrinoJson} is the one the driver produces for that
 * variant type: {@code Variant.toObject()} yields {@link Instant} for a timestamp with a zone,
 * {@link LocalDateTime} for one without, {@link LocalDate}, {@link LocalTime}, {@link BigDecimal} and
 * {@code byte[]} for the rest — which is why the offset a timestamp is rendered at survives the trip.
 */
class TrinoJsonVariantParityTest {

    private static final long MICROS_PER_SECOND = 1_000_000L;

    /** Builds a single-value variant. */
    private static Variant variant(Consumer<VariantBuilder> append) {
        VariantBuilder builder = new VariantBuilder();
        append.accept(builder);
        return builder.build();
    }

    /** Renders one leaf both ways and asserts the two agree. */
    private static void assertParity(Consumer<VariantBuilder> append, Object decoded) throws IOException {
        String viaFileSystem = VariantJsonWriter.toJson(variant(append));
        String viaTrino = TrinoJson.render(decoded);
        assertThat(viaTrino).isEqualTo(viaFileSystem);
    }

    /** The case that prompted this: a temporal leaf, rendered at UTC with an offset on both paths. */
    @Test
    void timestampWithAZoneAgrees() throws IOException {
        Instant instant = Instant.parse("2026-08-28T12:34:56.123456Z");
        long micros = instant.getEpochSecond() * MICROS_PER_SECOND + instant.getNano() / 1000L;
        assertParity(b -> b.appendTimestampTz(micros), instant);
    }

    @Test
    void timestampWithoutAZoneAgrees() throws IOException {
        LocalDateTime dateTime = LocalDateTime.parse("2026-08-28T12:34:56.123456");
        long micros = dateTime.toEpochSecond(java.time.ZoneOffset.UTC) * MICROS_PER_SECOND
                + dateTime.getNano() / 1000L;
        assertParity(b -> b.appendTimestampNtz(micros), dateTime);
    }

    @Test
    void wholeSecondTimestampsAgree() throws IOException {
        Instant instant = Instant.parse("2026-08-28T12:34:56Z");
        assertParity(b -> b.appendTimestampTz(instant.getEpochSecond() * MICROS_PER_SECOND), instant);
    }

    @Test
    void dateAgrees() throws IOException {
        LocalDate date = LocalDate.parse("2026-08-28");
        assertParity(b -> b.appendDate((int) date.toEpochDay()), date);
    }

    @Test
    void timeAgrees() throws IOException {
        LocalTime time = LocalTime.parse("12:34:56.123456");
        assertParity(b -> b.appendTime(time.toNanoOfDay() / 1000L), time);
    }

    /** The leaf type that has no flat GeoTools binding; both paths keep it an unquoted number. */
    @Test
    void decimalAgrees() throws IOException {
        BigDecimal decimal = new BigDecimal("1.50");
        assertParity(b -> b.appendDecimal(decimal), decimal);
        assertThat(TrinoJson.render(decimal)).doesNotContain("\"");
    }

    @Test
    void scalarsAgree() throws IOException {
        assertParity(b -> b.appendString("MMSI"), "MMSI");
        assertParity(b -> b.appendBoolean(true), true);
        assertParity(b -> b.appendLong(1200L), 1200L);
        assertParity(b -> b.appendInt(7), 7);
        assertParity(b -> b.appendDouble(0.75d), 0.75d);
        assertParity(b -> b.appendNull(), null);
    }

    /**
     * A float stays a float on both sides. This is the case that a plausible-looking widening to
     * {@code double} gets wrong, and it fails here rather than in a diff of two exports.
     */
    @Test
    void realAgrees() throws IOException {
        assertParity(b -> b.appendFloat(0.1f), 0.1f);
    }

    @Test
    void nonFiniteDoublesAgree() throws IOException {
        assertParity(b -> b.appendDouble(Double.NaN), Double.NaN);
        assertParity(b -> b.appendDouble(Double.POSITIVE_INFINITY), Double.POSITIVE_INFINITY);
    }

    @Test
    void binaryAgrees() throws IOException {
        byte[] bytes = {1, 2, 3, 4};
        assertParity(b -> b.appendBinary(ByteBuffer.wrap(bytes)), bytes);
    }

    /**
     * The {@code parametrics} shape end to end. Field order is the variant's, which is the order the
     * driver hands back too, so the objects compared here are built in that same order.
     */
    @Test
    void nestedObjectWithATemporalLeafAgrees() throws IOException {
        Instant observed = Instant.parse("2026-08-28T12:34:56.123456Z");
        long micros = observed.getEpochSecond() * MICROS_PER_SECOND + observed.getNano() / 1000L;

        Map<String, Object> decoded = new LinkedHashMap<>();
        decoded.put("first_observed", observed);
        decoded.put("frequency_hz", new BigDecimal("1200.50"));

        assertParity(b -> {
            VariantObjectBuilder object = b.startObject();
            object.appendKey("first_observed");
            object.appendTimestampTz(micros);
            object.appendKey("frequency_hz");
            object.appendDecimal(new BigDecimal("1200.50"));
            b.endObject();
        }, decoded);
    }
}
