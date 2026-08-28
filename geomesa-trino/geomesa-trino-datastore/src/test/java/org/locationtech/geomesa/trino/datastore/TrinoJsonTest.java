/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.trino.datastore;

import io.trino.jdbc.TrinoIntervalDayTime;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.math.BigDecimal;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The rendered form of every leaf type the Trino driver can put inside structural data.
 *
 * <p>These are exact-string assertions on purpose. The same document is reachable through the
 * FileSystem DataStore, and the two renderings have to agree; {@link TrinoJsonVariantParityTest}
 * checks them against each other, while the expectations here pin what that agreement is so a
 * change to either side fails loudly rather than drifting.
 */
class TrinoJsonTest {

    private static final Instant INSTANT = Instant.parse("2026-08-28T12:34:56.123456Z");

    @Test
    void nullRendersAsTheJsonLiteral() throws IOException {
        assertThat(TrinoJson.render(null)).isEqualTo("null");
    }

    @Test
    void stringsAndBooleansRenderAsThemselves() throws IOException {
        assertThat(TrinoJson.render("MMSI")).isEqualTo("\"MMSI\"");
        assertThat(TrinoJson.render(true)).isEqualTo("true");
    }

    @Test
    void integralNumbersRenderUnquoted() throws IOException {
        assertThat(TrinoJson.render((byte) 7)).isEqualTo("7");
        assertThat(TrinoJson.render((short) 7)).isEqualTo("7");
        assertThat(TrinoJson.render(7)).isEqualTo("7");
        assertThat(TrinoJson.render(7L)).isEqualTo("7");
    }

    @Test
    void doublesRenderUnquoted() throws IOException {
        assertThat(TrinoJson.render(0.75d)).isEqualTo("0.75");
    }

    /**
     * A {@code real} leaf arrives as a {@link Float} and stays one. Widening it to {@code double} first
     * would render {@code 0.1f} as {@code 0.10000000149011612}, which is a different string than the
     * variant writer's, and a longer one that is no more accurate.
     */
    @Test
    void realsRenderAtFloatWidth() throws IOException {
        assertThat(TrinoJson.render(0.1f)).isEqualTo("0.1");
    }

    /** JSON has no NaN or infinity token, so these become null rather than an unparseable document. */
    @Test
    void nonFiniteNumbersRenderAsNull() throws IOException {
        assertThat(TrinoJson.render(Double.NaN)).isEqualTo("null");
        assertThat(TrinoJson.render(Double.POSITIVE_INFINITY)).isEqualTo("null");
        assertThat(TrinoJson.render(Float.NEGATIVE_INFINITY)).isEqualTo("null");
    }

    @Test
    void decimalsRenderUnquotedAtTheScaleTheyCarry() throws IOException {
        assertThat(TrinoJson.render(new BigDecimal("1.50"))).isEqualTo("1.50");
        assertThat(TrinoJson.render(new BigDecimal("-0.0000001"))).isEqualTo("-1E-7");
    }

    /**
     * {@code timestamp with time zone} at UTC, which is how the offset is always written even when the
     * reader's own zone is something else — the previous {@code ObjectMapper} rendering produced
     * {@code 2026-08-28T12:34:56.123+00:00} here, at a fixed three fractional digits.
     */
    @Test
    void instantsRenderAsIsoOffsetAtUtc() throws IOException {
        assertThat(TrinoJson.render(INSTANT)).isEqualTo("\"2026-08-28T12:34:56.123456Z\"");
    }

    /** The driver's representation for both timestamp flavors; the instant it holds is what is rendered. */
    @Test
    void sqlTimestampsRenderAsIsoOffsetAtUtc() throws IOException {
        assertThat(TrinoJson.render(Timestamp.from(INSTANT))).isEqualTo("\"2026-08-28T12:34:56.123456Z\"");
    }

    /** Offsets are normalized rather than preserved, so the same instant is one string however it arrives. */
    @Test
    void offsetAndZonedDateTimesNormalizeToUtc() throws IOException {
        OffsetDateTime offset = INSTANT.atOffset(ZoneOffset.ofHours(-5));
        ZonedDateTime zoned = INSTANT.atZone(ZoneOffset.ofHours(9));
        assertThat(TrinoJson.render(offset)).isEqualTo("\"2026-08-28T12:34:56.123456Z\"");
        assertThat(TrinoJson.render(zoned)).isEqualTo("\"2026-08-28T12:34:56.123456Z\"");
    }

    /** A variant {@code timestamp} without a zone has no offset to write, and does not gain one. */
    @Test
    void localDateTimesRenderWithoutAnOffset() throws IOException {
        assertThat(TrinoJson.render(LocalDateTime.parse("2026-08-28T12:34:56.123456")))
                .isEqualTo("\"2026-08-28T12:34:56.123456\"");
    }

    @Test
    void datesAndTimesRenderAsIsoLocal() throws IOException {
        assertThat(TrinoJson.render(LocalDate.parse("2026-08-28"))).isEqualTo("\"2026-08-28\"");
        assertThat(TrinoJson.render(LocalTime.parse("12:34:56"))).isEqualTo("\"12:34:56\"");
        assertThat(TrinoJson.render(java.sql.Date.valueOf("2026-08-28"))).isEqualTo("\"2026-08-28\"");
        assertThat(TrinoJson.render(Time.valueOf("12:34:56"))).isEqualTo("\"12:34:56\"");
    }

    @Test
    void varbinaryRendersAsBase64() throws IOException {
        assertThat(TrinoJson.render(new byte[] {1, 2, 3})).isEqualTo("\"AQID\"");
    }

    @Test
    void uuidsRenderAsTheirCanonicalText() throws IOException {
        UUID uuid = UUID.fromString("28f5b1ce-1c1a-4b3f-9f6a-3f2e1d0c9b8a");
        assertThat(TrinoJson.render(uuid)).isEqualTo("\"28f5b1ce-1c1a-4b3f-9f6a-3f2e1d0c9b8a\"");
    }

    @Test
    void mapsRenderAsObjectsInIterationOrder() throws IOException {
        Map<String, Object> map = new LinkedHashMap<>();
        map.put("realm", "MMSI");
        map.put("selector", "123");
        assertThat(TrinoJson.render(map)).isEqualTo("{\"realm\":\"MMSI\",\"selector\":\"123\"}");
    }

    @Test
    void listsRenderAsArrays() throws IOException {
        assertThat(TrinoJson.render(List.of(1, 2, 3))).isEqualTo("[1,2,3]");
    }

    @Test
    void nullsInsideContainersAreKept() throws IOException {
        Map<String, Object> map = new LinkedHashMap<>();
        map.put("present", 1);
        map.put("absent", null);
        assertThat(TrinoJson.render(map)).isEqualTo("{\"present\":1,\"absent\":null}");
        assertThat(TrinoJson.render(Arrays.asList("a", null))).isEqualTo("[\"a\",null]");
    }

    /** The shape {@code parametrics} has: a row of scalars, one of them temporal. */
    @Test
    void nestingRecursesThroughObjectsAndArrays() throws IOException {
        Map<String, Object> parametrics = new LinkedHashMap<>();
        parametrics.put("first_observed", INSTANT);
        parametrics.put("readings", List.of(Map.of("hz", 1200L)));
        assertThat(TrinoJson.render(parametrics)).isEqualTo(
                "{\"first_observed\":\"2026-08-28T12:34:56.123456Z\",\"readings\":[{\"hz\":1200}]}");
    }

    @Test
    void quotesAndControlCharactersAreEscaped() throws IOException {
        assertThat(TrinoJson.render("a\"b\\c\nd")).isEqualTo("\"a\\\"b\\\\c\\nd\"");
    }

    /**
     * The exact-class table does not have a row for every {@link CharSequence}, {@link Number} or
     * {@link java.util.Date}, only for the classes the driver actually produces. Anything else
     * recognizable by supertype falls to the ordered supertype rows rather than to the warning.
     */
    @Test
    void leavesRecognizableOnlyBySupertypeStillRender() throws IOException {
        assertThat(TrinoJson.render(new StringBuilder("MMSI"))).isEqualTo("\"MMSI\"");
        assertThat(TrinoJson.render(new AtomicInteger(7))).isEqualTo("7");
        // a java.util.Date holds only milliseconds, so the microseconds of INSTANT are truncated
        assertThat(TrinoJson.render(new java.util.Date(INSTANT.toEpochMilli())))
                .isEqualTo("\"2026-08-28T12:34:56.123Z\"");
    }

    /**
     * A {@code java.sql.Date} is a {@link java.util.Date}, and the two render differently. Its own row
     * has to win over the supertype row, which is the hazard the exact-class lookup removes.
     */
    @Test
    void anExactRowWinsOverTheSupertypeRow() throws IOException {
        assertThat(TrinoJson.render(java.sql.Date.valueOf("2026-08-28"))).isEqualTo("\"2026-08-28\"");
        assertThat(TrinoJson.render(Time.valueOf("12:34:56"))).isEqualTo("\"12:34:56\"");
    }

    /**
     * An {@code interval day to second} leaf is a real driver type with no branch of its own: quoted and
     * logged, rather than left to break the document or abort the scan.
     */
    @Test
    void unknownLeafTypesRenderAsStrings() throws IOException {
        String rendered = TrinoJson.render(new TrinoIntervalDayTime(0, 0, 0, 5, 0));
        assertThat(rendered).startsWith("\"").endsWith("\"").contains("5");
    }
}
