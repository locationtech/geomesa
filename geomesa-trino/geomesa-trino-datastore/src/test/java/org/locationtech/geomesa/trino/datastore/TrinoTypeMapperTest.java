/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.trino.datastore;

import org.geotools.api.feature.type.AttributeDescriptor;
import org.junit.jupiter.api.Test;
import org.locationtech.jts.geom.Geometry;

import java.sql.Types;
import java.util.Date;

import static org.assertj.core.api.Assertions.assertThat;

class TrinoTypeMapperTest {

    @Test
    void varcharMapsToString() {
        AttributeDescriptor d = TrinoTypeMapper.toDescriptor("fid", Types.VARCHAR, false, false, 0);
        assertThat(d.getType().getBinding()).isEqualTo(String.class);
        assertThat(d.getLocalName()).isEqualTo("fid");
    }

    @Test
    void bigintMapsToLong() {
        AttributeDescriptor d = TrinoTypeMapper.toDescriptor("count_col", Types.BIGINT, false, false, 0);
        assertThat(d.getType().getBinding()).isEqualTo(Long.class);
    }

    @Test
    void integerMapsToInteger() {
        AttributeDescriptor d = TrinoTypeMapper.toDescriptor("taxi_id", Types.INTEGER, false, false, 0);
        assertThat(d.getType().getBinding()).isEqualTo(Integer.class);
    }

    @Test
    void doubleMapsToDouble() {
        AttributeDescriptor d = TrinoTypeMapper.toDescriptor("value", Types.DOUBLE, false, false, 0);
        assertThat(d.getType().getBinding()).isEqualTo(Double.class);
    }

    @Test
    void booleanMapsToBoolean() {
        AttributeDescriptor d = TrinoTypeMapper.toDescriptor("active", Types.BOOLEAN, false, false, 0);
        assertThat(d.getType().getBinding()).isEqualTo(Boolean.class);
    }

    @Test
    void timestampWithTimezoneMapsToDate() {
        AttributeDescriptor d = TrinoTypeMapper.toDescriptor("dtg", Types.TIMESTAMP_WITH_TIMEZONE, false, false, 0);
        assertThat(d.getType().getBinding()).isEqualTo(Date.class);
    }

    @Test
    void varbinaryWithGeometryFlagMapsToGeometry() {
        AttributeDescriptor d = TrinoTypeMapper.toDescriptor("geom_wkb", Types.VARBINARY, true, false, 4326);
        assertThat(d.getType().getBinding()).isEqualTo(Geometry.class);
        assertThat(d).isInstanceOf(org.geotools.api.feature.type.GeometryDescriptor.class);
        assertThat(((org.geotools.api.feature.type.GeometryDescriptor) d)
            .getCoordinateReferenceSystem())
            .isEqualTo(org.geotools.referencing.crs.DefaultGeographicCRS.WGS84);
    }

    @Test
    void varbinaryWithPointFlagMapsToPoint() {
        // A __X_z2__ companion marks a point-only column; discovery passes isPoint=true
        // so the descriptor binds Point (enables the rectangle/point bbox fast path).
        AttributeDescriptor d = TrinoTypeMapper.toDescriptor("center", Types.VARBINARY, true, true, 4326);
        assertThat(d.getType().getBinding()).isEqualTo(org.locationtech.jts.geom.Point.class);
        assertThat(d).isInstanceOf(org.geotools.api.feature.type.GeometryDescriptor.class);
    }

    @Test
    void varbinaryWithoutGeometryFlagMapsToBytesArray() {
        AttributeDescriptor d = TrinoTypeMapper.toDescriptor("raw_data", Types.VARBINARY, false, false, 0);
        assertThat(d.getType().getBinding()).isEqualTo(byte[].class);
    }

    @Test
    void hidesBookkeepingAndCompanionColumns() {
        // __fid__/__vis__ and every geom companion are hidden — for any geom name,
        // not just the legacy "geom" (regression: the old hardcoded set leaked
        // __*_z2__/__*_xz2__ and multi-geom companions into the schema).
        assertThat(TrinoTypeMapper.isHidden("__fid__")).isTrue();
        assertThat(TrinoTypeMapper.isHidden("__vis__")).isTrue();
        assertThat(TrinoTypeMapper.isHidden("__geom_bbox__")).isTrue();
        assertThat(TrinoTypeMapper.isHidden("__geom_z2__")).isTrue();
        assertThat(TrinoTypeMapper.isHidden("__center_bbox__")).isTrue();
        assertThat(TrinoTypeMapper.isHidden("__ellipse_xz2__")).isTrue();
    }

    @Test
    void doesNotHideGeometryBaseOrPlainColumns() {
        assertThat(TrinoTypeMapper.isHidden("geom")).isFalse();
        assertThat(TrinoTypeMapper.isHidden("center")).isFalse();
        assertThat(TrinoTypeMapper.isHidden("sensor_id")).isFalse();
    }
}
