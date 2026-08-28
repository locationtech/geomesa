/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.trino.datastore;

import io.trino.jdbc.Row;
import org.geotools.api.feature.simple.SimpleFeature;
import org.geotools.api.feature.simple.SimpleFeatureType;
import org.geotools.api.feature.type.AttributeDescriptor;
import org.geotools.feature.simple.SimpleFeatureTypeBuilder;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.lang.reflect.Proxy;
import java.math.BigDecimal;
import java.sql.ResultSet;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * What a single row becomes, driven through the reader itself rather than through its helpers, so the
 * attribute a consumer actually sees is what is asserted: the descriptor from {@link TrinoTypeMapper},
 * the classification the reader derives from it, and the conversion it applies.
 */
class TrinoFeatureReaderTest {

    /**
     * A one-row {@link ResultSet} over a column-name map. Only the accessors the reader calls are
     * implemented; anything else fails loudly rather than returning a plausible default.
     */
    private static ResultSet resultSet(Map<String, Object> row) {
        AtomicBoolean read = new AtomicBoolean(false);
        return (ResultSet) Proxy.newProxyInstance(
                TrinoFeatureReaderTest.class.getClassLoader(),
                new Class<?>[] { ResultSet.class },
                (proxy, method, args) -> switch (method.getName()) {
                    case "next" -> read.compareAndSet(false, true);
                    case "getObject", "getBytes", "getTimestamp" -> row.get((String) args[0]);
                    case "getString" -> {
                        Object value = row.get((String) args[0]);
                        yield value == null ? null : value.toString();
                    }
                    default -> throw new UnsupportedOperationException(method.getName());
                });
    }

    /** Reads the single row of {@code values} as a feature whose type is the given columns. */
    private static SimpleFeature readOne(Map<String, Object> values, AttributeDescriptor... descriptors)
            throws IOException {
        SimpleFeatureTypeBuilder builder = new SimpleFeatureTypeBuilder();
        builder.setName("t");
        for (AttributeDescriptor descriptor : descriptors) {
            builder.add(descriptor);
        }
        SimpleFeatureType sft = builder.buildFeatureType();
        TrinoFeatureReader reader =
                new TrinoFeatureReader(sft, null, null, resultSet(values), null, null);
        assertThat(reader.hasNext()).isTrue();
        return reader.next();
    }

    private static AttributeDescriptor descriptor(String name, int sqlType, String typeName) {
        return TrinoTypeMapper.toDescriptor(name, sqlType, typeName, false, null, 0);
    }

    private static Map<String, Object> row(String column, Object value) {
        Map<String, Object> values = new HashMap<>();
        values.put(column, value);
        return values;
    }

    @Test
    void decimalArrivesAsItsExactDigits() throws IOException {
        SimpleFeature feature = readOne(row("amount", new BigDecimal("1.50")),
                descriptor("amount", Types.DECIMAL, "decimal(10,2)"));
        assertThat(feature.getAttribute("amount")).isEqualTo("1.50");
    }

    /**
     * Plain notation, not {@code BigDecimal.toString()}'s exponent form. The attribute is text a CQL
     * predicate or a WFS response is written against, and {@code 1E-7} would not match the value as
     * anyone would write it.
     */
    @Test
    void smallDecimalsKeepPlainNotation() throws IOException {
        SimpleFeature feature = readOne(row("amount", new BigDecimal("0.0000001")),
                descriptor("amount", Types.DECIMAL, "decimal(38,10)"));
        assertThat(feature.getAttribute("amount")).isEqualTo("0.0000001");
    }

    @Test
    void aNullDecimalStaysNull() throws IOException {
        SimpleFeature feature = readOne(row("amount", null),
                descriptor("amount", Types.DECIMAL, "decimal(10,2)"));
        assertThat(feature.getAttribute("amount")).isNull();
    }

    /**
     * The end-to-end shape of issue one: a temporal leaf inside a {@code row} column, which reaches the
     * attribute as a rendered document rather than as Jackson's date format.
     */
    @Test
    void temporalLeavesInsideARowRenderWithAnIsoOffset() throws IOException {
        Timestamp observed = Timestamp.from(Instant.parse("2026-08-28T12:34:56.123456Z"));
        Row value = Row.builder()
                .addField("first_observed", observed)
                .addField("frequency_hz", new BigDecimal("1200.50"))
                .build();
        SimpleFeature feature = readOne(row("parametrics", value),
                descriptor("parametrics", Types.JAVA_OBJECT,
                        "row(first_observed timestamp(6) with time zone, frequency_hz decimal(10,2))"));
        assertThat(feature.getAttribute("parametrics")).isEqualTo(
                "{\"first_observed\":\"2026-08-28T12:34:56.123456Z\",\"frequency_hz\":1200.50}");
    }

    /** A {@code List} attribute is assigned, not rendered; only {@code json=true} attributes are documents. */
    @Test
    void arrayOfScalarsArrivesAsAList() throws IOException {
        SimpleFeature feature = readOne(row("ids", List.of("a", "b")),
                descriptor("ids", Types.ARRAY, "array(varchar)"));
        assertThat(feature.getAttribute("ids")).isEqualTo(List.of("a", "b"));
    }
}
