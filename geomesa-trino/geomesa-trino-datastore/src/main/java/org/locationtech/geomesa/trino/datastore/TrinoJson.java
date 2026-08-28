/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.trino.datastore;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.StringWriter;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.TemporalAccessor;
import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * Renders a structural value as the JSON document a {@code json=true} attribute promises.
 *
 * <p>How each leaf is formatted is a compatibility contract rather than a local choice. The same
 * document can reach a consumer two ways: through the FileSystem DataStore, where the Parquet
 * {@code variant} holding it is rendered by {@code VariantJsonWriter}, or through this DataStore,
 * where the Trino JDBC driver decodes the variant and this class renders the result. Anything that
 * compares the two - a parity test, a cache key, a diff between an FSDS export and a Trino export -
 * needs the same string from both, so the table below reproduces that writer's output for the
 * corresponding variant type:
 *
 * <ul>
 *   <li>timestamps that carry an offset: {@link DateTimeFormatter#ISO_OFFSET_DATE_TIME} at UTC,
 *       so a zero offset prints as {@code Z};</li>
 *   <li>timestamps that do not: {@link DateTimeFormatter#ISO_LOCAL_DATE_TIME};</li>
 *   <li>dates and times: {@link DateTimeFormatter#ISO_LOCAL_DATE} and
 *       {@link DateTimeFormatter#ISO_LOCAL_TIME};</li>
 *   <li>{@code decimal}: an unquoted JSON number, at the scale it was stored with;</li>
 *   <li>{@code real}: written as a {@code float}, not widened - that writer reaches Gson's
 *       {@code value(float)} overload, so {@code 0.1f} is {@code 0.1} and not the
 *       {@code 0.10000000149011612} a widening to {@code double} would produce;</li>
 *   <li>NaN and the infinities: {@code null}, which is what that writer emits, JSON having no
 *       literal for them;</li>
 *   <li>{@code varbinary}: base64 in the standard padded alphabet with no line breaks, matching
 *       {@code java.util.Base64.getEncoder()}.</li>
 * </ul>
 *
 */
final class TrinoJson {

    private static final Logger LOG = LoggerFactory.getLogger(TrinoJson.class);

    /** Stateless, thread-safe; a generator is created per call. */
    private static final JsonFactory FACTORY = new JsonFactory();

    @FunctionalInterface
    private interface JsonLeaf<T> {
        void write(JsonGenerator gen, T value) throws IOException;
    }

    private static final JsonLeaf<Number> INTEGRAL = (gen, value) -> gen.writeNumber(value.longValue());

    /**
     * Keyed on the exact class, because that is what the driver hands back and an exact lookup has no
     * precedence to get wrong: {@link Timestamp}, {@link java.sql.Date} and {@link Time} all extend
     * {@link Date} and each renders differently, which an ordered chain of {@code instanceof} tests has
     * to encode as a comment and this does not.
     */
    private static final Map<Class<?>, JsonLeaf<Object>> LEAVES = Map.ofEntries(
            leaf(String.class,         JsonGenerator::writeString),
            leaf(Boolean.class,        JsonGenerator::writeBoolean),
            leaf(Byte.class,           INTEGRAL),
            leaf(Short.class,          INTEGRAL),
            leaf(Integer.class,        INTEGRAL),
            leaf(Long.class,           INTEGRAL),
            leaf(Float.class,          TrinoJson::writeFloat),
            leaf(Double.class,         TrinoJson::writeDouble),
            leaf(BigDecimal.class,     JsonGenerator::writeNumber),
            leaf(BigInteger.class,     JsonGenerator::writeNumber),
            leaf(byte[].class,         JsonGenerator::writeBinary),
            leaf(UUID.class,           (gen, value) -> gen.writeString(value.toString())),
            leaf(Instant.class,        TrinoJson::writeUtc),
            leaf(OffsetDateTime.class, TrinoJson::writeUtc),
            leaf(ZonedDateTime.class,  TrinoJson::writeUtc),
            leaf(Timestamp.class,      (gen, value) -> writeUtc(gen, value.toInstant())),
            leaf(LocalDateTime.class,  (gen, value) -> writeIso(gen, value, DateTimeFormatter.ISO_LOCAL_DATE_TIME)),
            leaf(LocalDate.class,      (gen, value) -> writeIso(gen, value, DateTimeFormatter.ISO_LOCAL_DATE)),
            leaf(LocalTime.class,      (gen, value) -> writeIso(gen, value, DateTimeFormatter.ISO_LOCAL_TIME)),
            leaf(java.sql.Date.class,  (gen, value) -> writeIso(gen, value.toLocalDate(), DateTimeFormatter.ISO_LOCAL_DATE)),
            leaf(Time.class,           (gen, value) -> writeIso(gen, value.toLocalTime(), DateTimeFormatter.ISO_LOCAL_TIME)));

    /**
     * Consulted when the exact class is not in the table, for a value that is still recognizable
     * by supertype. Order drives precedence, so the first assignable entry wins.
     */
    private static final List<Map.Entry<Class<?>, JsonLeaf<Object>>> SUPERTYPES = List.of(
            leaf(CharSequence.class, (gen, value) -> gen.writeString(value.toString())),
            leaf(Date.class,         (gen, value) -> writeUtc(gen, value.toInstant())),
            leaf(Number.class,       INTEGRAL));

    private static final JsonLeaf<Object> UNRECOGNIZED = (gen, value) -> {
        LOG.warn("No JSON rendering for a leaf of type {}; writing it as a string", value.getClass().getName());
        gen.writeString(value.toString());
    };

    private TrinoJson() {}

    /**
     * Renders a value already flattened by {@link TrinoFeatureReader#normalize(Object)}.
     *
     * @param value normalized value, possibly null
     * @return the JSON document for it
     */
    static String render(Object value) throws IOException {
        StringWriter out = new StringWriter();
        try (JsonGenerator gen = FACTORY.createGenerator(out)) {
            write(gen, value);
        }
        return out.toString();
    }

    private static void write(JsonGenerator gen, Object value) throws IOException {
        if (value == null) {
            gen.writeNull();
        } else if (value instanceof Map<?, ?> fields) {
            gen.writeStartObject();
            for (Map.Entry<?, ?> field : fields.entrySet()) {
                gen.writeFieldName(String.valueOf(field.getKey()));
                write(gen, field.getValue());
            }
            gen.writeEndObject();
        } else if (value instanceof Collection<?> elements) {
            gen.writeStartArray();
            for (Object element : elements) {
                write(gen, element);
            }
            gen.writeEndArray();
        } else {
            writerFor(value.getClass()).write(gen, value);
        }
    }

    private static JsonLeaf<Object> writerFor(Class<?> type) {
        JsonLeaf<Object> exact = LEAVES.get(type);
        return exact != null ? exact
                : SUPERTYPES.stream()
                            .filter(candidate -> candidate.getKey().isAssignableFrom(type))
                            .findFirst()
                            .map(Map.Entry::getValue)
                            .orElse(UNRECOGNIZED);
    }

    @SuppressWarnings("unchecked")
    private static <T> Map.Entry<Class<?>, JsonLeaf<Object>> leaf(Class<T> type, JsonLeaf<? super T> writer) {
        return Map.entry(type, (gen, value) -> writer.write(gen, (T) value));
    }

    private static void writeDouble(JsonGenerator gen, double value) throws IOException {
        if (Double.isNaN(value) || Double.isInfinite(value)) {
            gen.writeNull();
        } else {
            gen.writeNumber(value);
        }
    }

    private static void writeFloat(JsonGenerator gen, float value) throws IOException {
        if (Float.isNaN(value) || Float.isInfinite(value)) {
            gen.writeNull();
        } else {
            gen.writeNumber(value);
        }
    }

    private static void writeUtc(JsonGenerator gen, TemporalAccessor value) throws IOException {
        writeIso(gen, Instant.from(value).atOffset(ZoneOffset.UTC), DateTimeFormatter.ISO_OFFSET_DATE_TIME);
    }

    private static void writeIso(JsonGenerator gen, TemporalAccessor value, DateTimeFormatter format)
            throws IOException {
        gen.writeString(format.format(value));
    }
}
