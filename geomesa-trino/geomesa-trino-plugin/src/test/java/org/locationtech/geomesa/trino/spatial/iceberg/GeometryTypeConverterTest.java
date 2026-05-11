/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.trino.spatial.iceberg;

import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class GeometryTypeConverterTest {

    private final GeometryTypeConverter converter = new GeometryTypeConverter();

    @Test
    void geometryTypeMapsToGeometryTypeName() {
        assertThat(converter.toTrinoTypeName(GeometryType.get())).isEqualTo("Geometry");
    }

    @Test
    void geometryTypeWithCustomSridStillMapsToGeometry() {
        assertThat(converter.toTrinoTypeName(GeometryType.of(32632))).isEqualTo("Geometry");
    }

    @Test
    void binaryTypeMapsToVarbinary() {
        assertThat(converter.toTrinoTypeName(Types.BinaryType.get())).isEqualTo("varbinary");
    }

    @Test
    void longTypeMapsToDefaultBigint() {
        assertThat(converter.toTrinoTypeName(Types.LongType.get())).isEqualTo("bigint");
    }

    @Test
    void stringTypeMapsToVarchar() {
        assertThat(converter.toTrinoTypeName(Types.StringType.get())).isEqualTo("varchar");
    }

    @Test
    void doubleTypeMapsToDouble() {
        assertThat(converter.toTrinoTypeName(Types.DoubleType.get())).isEqualTo("double");
    }

    @Test
    void geometryTypeNameRoundTrip() {
        assertThat(converter.isGeometryType("Geometry")).isTrue();
        assertThat(converter.isGeometryType("geometry")).isTrue();
        assertThat(converter.isGeometryType("GEOMETRY")).isTrue();
        assertThat(converter.isGeometryType("varbinary")).isFalse();
        assertThat(converter.isGeometryType("varchar")).isFalse();
    }

    @Test
    void geometryTypeNotMappedToVarbinaryDespiteBinaryTypeId() {
        // GeometryType.typeId() returns BINARY — without the instanceof guard first,
        // this would fall through to the switch and return "varbinary".
        String result = converter.toTrinoTypeName(GeometryType.get());
        assertThat(result).isNotEqualTo("varbinary");
        assertThat(result).isEqualTo("Geometry");
    }

    @Test
    void timestampWithZoneMapsToTimestampWithTimeZone() {
        assertThat(converter.toTrinoTypeName(Types.TimestampType.withZone()))
            .isEqualTo("timestamp(6) with time zone");
    }

    @Test
    void timestampWithoutZoneMapsToTimestamp() {
        assertThat(converter.toTrinoTypeName(Types.TimestampType.withoutZone()))
            .isEqualTo("timestamp(6)");
    }
}
