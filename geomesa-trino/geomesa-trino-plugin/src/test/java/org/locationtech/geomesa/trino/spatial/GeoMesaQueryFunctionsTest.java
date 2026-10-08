/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.trino.spatial;

import io.airlift.slice.Slices;
import io.trino.metadata.InternalFunctionBundle;
import io.trino.spi.type.DateTimeEncoding;
import io.trino.spi.type.TimeZoneKey;
import io.trino.spi.type.UuidType;
import org.geotools.factory.CommonFactoryFinder;
import org.geotools.feature.simple.SimpleFeatureBuilder;
import org.geotools.feature.simple.SimpleFeatureTypeBuilder;
import org.junit.jupiter.api.Test;

import java.util.Date;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

class GeoMesaQueryFunctionsTest {

    private static Object evaluate(String name, Object... args) {
        var ff = CommonFactoryFinder.getFilterFactory();
        var expressions = java.util.Arrays.stream(args).map(ff::literal)
            .toArray(org.geotools.api.filter.expression.Expression[]::new);
        return ff.function(name, expressions).evaluate(new Object());
    }

    @Test
    void functionsAreRegisteredWithValidTrinoSignatures() {
        assertThat(new SpatialIcebergPlugin().getFunctions()).contains(GeoMesaQueryFunctions.class);
        assertThat(InternalFunctionBundle.builder().functions(GeoMesaQueryFunctions.class).build().getFunctions())
            .hasSize(8);
    }

    @Test
    void hashOverloadsMatchGeoMesaIncludingNegativeAndNonAsciiValues() {
        for (String value : new String[]{"", "test", "é漢字", "a\u0000b"}) {
            assertThat(GeoMesaQueryFunctions.hashString(Slices.utf8Slice(value)))
                .isEqualTo(((Number) evaluate("murmurHash", value)).longValue());
        }
        for (long value : new long[]{0, 1, -1, Long.MIN_VALUE, Long.MAX_VALUE}) {
            assertThat(GeoMesaQueryFunctions.hashLong(value))
                .isEqualTo(((Number) evaluate("murmurHash", value)).longValue());
        }
        assertThat(GeoMesaQueryFunctions.hashLong(-123))
            .isEqualTo(((Number) evaluate("murmurHash", -123)).longValue());
        for (double value : new double[]{0.0, -0.0, -123.25, Double.NaN, Double.POSITIVE_INFINITY}) {
            assertThat(GeoMesaQueryFunctions.hashDouble(value))
                .isEqualTo(((Number) evaluate("murmurHash", value)).longValue());
        }
        for (float value : new float[]{0.0f, -0.0f, 123.25f, Float.NaN, Float.NEGATIVE_INFINITY}) {
            assertThat(GeoMesaQueryFunctions.hashReal(Float.floatToIntBits(value)))
                .isEqualTo(((Number) evaluate("murmurHash", value)).longValue());
        }
        byte[] bytes = new byte[]{0, 1, -1, -128, 127};
        assertThat(GeoMesaQueryFunctions.hashBytes(Slices.wrappedBuffer(bytes)))
            .isEqualTo(((Number) evaluate("murmurHash", (Object) bytes)).longValue());
        for (long millis : new long[]{0, -1, 1234567890123L}) {
            long encoded = DateTimeEncoding.packDateTimeWithZone(millis, TimeZoneKey.UTC_KEY);
            assertThat(GeoMesaQueryFunctions.hashDate(encoded))
                .isEqualTo(((Number) evaluate("murmurHash", new Date(millis))).longValue());
        }
        UUID uuid = UUID.fromString("fedcba98-7654-3210-ffff-0123456789ab");
        assertThat(GeoMesaQueryFunctions.hashUuid(UuidType.javaUuidToTrinoUuid(uuid)))
            .isEqualTo(((Number) evaluate("murmurHash", uuid)).longValue());
    }

    @Test
    void bucketHashUsesPositiveMaskBeforeModulo() {
        for (String value : new String[]{"", "test", "é漢字"}) {
            for (int modulo : new int[]{1, 3, 128}) {
                long actual = (GeoMesaQueryFunctions.hashString(Slices.utf8Slice(value)) & Integer.MAX_VALUE) % modulo;
                assertThat(actual).isEqualTo(((Number) evaluate("bucketHash", value, modulo)).longValue());
            }
        }
    }

    @Test
    void proxyIdMatchesFeatureIdAndUuidModes() {
        var ff = CommonFactoryFinder.getFilterFactory();
        var builder = new SimpleFeatureTypeBuilder();
        builder.setName("test");
        builder.add("name", String.class);
        var sft = builder.buildFeatureType();
        for (boolean uuid : new boolean[]{false, true}) {
            sft.getUserData().put("geomesa.fid.uuid", uuid);
            String id = uuid ? "fedcba98-7654-3210-ffff-0123456789ab" : "feature-漢字";
            var feature = SimpleFeatureBuilder.build(sft, new Object[]{"name"}, id);
            assertThat(GeoMesaQueryFunctions.proxyId(Slices.utf8Slice(id), uuid))
                .isEqualTo(((Number) ff.function("proxyId").evaluate(feature)).longValue());
        }
    }

}
