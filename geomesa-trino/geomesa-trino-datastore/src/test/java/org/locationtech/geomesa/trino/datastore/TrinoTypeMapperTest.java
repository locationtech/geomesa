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
        AttributeDescriptor d = TrinoTypeMapper.toDescriptor("fid", Types.VARCHAR, false, null, 0);
        assertThat(d.getType().getBinding()).isEqualTo(String.class);
        assertThat(d.getLocalName()).isEqualTo("fid");
    }

    @Test
    void bigintMapsToLong() {
        AttributeDescriptor d = TrinoTypeMapper.toDescriptor("count_col", Types.BIGINT, false, null, 0);
        assertThat(d.getType().getBinding()).isEqualTo(Long.class);
    }

    @Test
    void integerMapsToInteger() {
        AttributeDescriptor d = TrinoTypeMapper.toDescriptor("taxi_id", Types.INTEGER, false, null, 0);
        assertThat(d.getType().getBinding()).isEqualTo(Integer.class);
    }

    @Test
    void doubleMapsToDouble() {
        AttributeDescriptor d = TrinoTypeMapper.toDescriptor("value", Types.DOUBLE, false, null, 0);
        assertThat(d.getType().getBinding()).isEqualTo(Double.class);
    }

    @Test
    void booleanMapsToBoolean() {
        AttributeDescriptor d = TrinoTypeMapper.toDescriptor("active", Types.BOOLEAN, false, null, 0);
        assertThat(d.getType().getBinding()).isEqualTo(Boolean.class);
    }

    @Test
    void timestampWithTimezoneMapsToDate() {
        AttributeDescriptor d = TrinoTypeMapper.toDescriptor("dtg", Types.TIMESTAMP_WITH_TIMEZONE, false, null, 0);
        assertThat(d.getType().getBinding()).isEqualTo(Date.class);
    }

    @Test
    void geometryBindingPassesThroughTheResolvedSubtype() {
        // The caller resolves the subtype (from the stored SFT or the heuristic); the mapper
        // binds exactly that. Covers the full JTS geometry hierarchy the SFT can declare.
        for (Class<?> subtype : java.util.List.of(
                org.locationtech.jts.geom.Point.class,
                org.locationtech.jts.geom.LineString.class,
                org.locationtech.jts.geom.Polygon.class,
                org.locationtech.jts.geom.MultiPoint.class,
                org.locationtech.jts.geom.MultiLineString.class,
                org.locationtech.jts.geom.MultiPolygon.class,
                org.locationtech.jts.geom.GeometryCollection.class,
                Geometry.class)) {
            AttributeDescriptor d = TrinoTypeMapper.toDescriptor("geom", Types.VARBINARY, true, subtype, 4326);
            assertThat(d.getType().getBinding()).as(subtype.getSimpleName()).isEqualTo(subtype);
            assertThat(d).isInstanceOf(org.geotools.api.feature.type.GeometryDescriptor.class);
        }
    }

    @Test
    void geometryDescriptorCarriesWgs84() {
        AttributeDescriptor d = TrinoTypeMapper.toDescriptor("geom_wkb", Types.VARBINARY, true, Geometry.class, 4326);
        assertThat(((org.geotools.api.feature.type.GeometryDescriptor) d)
            .getCoordinateReferenceSystem())
            .isEqualTo(org.geotools.referencing.crs.DefaultGeographicCRS.WGS84);
    }

    @Test
    void nullOrNonGeometryBindingDefaultsToGenericGeometry() {
        // Defensive: a null binding (or a non-Geometry class slipping through) falls back to
        // generic Geometry rather than producing an invalid descriptor.
        assertThat(TrinoTypeMapper.toDescriptor("geom", Types.VARBINARY, true, null, 4326)
            .getType().getBinding()).isEqualTo(Geometry.class);
        assertThat(TrinoTypeMapper.toDescriptor("geom", Types.VARBINARY, true, String.class, 4326)
            .getType().getBinding()).isEqualTo(Geometry.class);
    }

    @Test
    void varbinaryWithoutGeometryFlagMapsToBytesArray() {
        AttributeDescriptor d = TrinoTypeMapper.toDescriptor("raw_data", Types.VARBINARY, false, null, 0);
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

    // ── structural types ──────────────────────────────────────────────────────
    //
    // The bare java.sql.Types code cannot tell array(varchar) from array(row(...)),
    // so these all hinge on the parameterized type name being supplied.

    @Test
    void arrayOfScalarMapsToListWithSubtype() {
        AttributeDescriptor d = TrinoTypeMapper.toDescriptor(
                "provenance_umr_ids", Types.ARRAY, "array(varchar)", false, null, 0);
        assertThat(d.getType().getBinding()).isEqualTo(java.util.List.class);
        assertThat(d.getUserData()).containsEntry(TrinoTypeMapper.OPT_SUBTYPE, "java.lang.String");
    }

    @Test
    void arrayElementTypeParametersAreIgnored() {
        AttributeDescriptor d = TrinoTypeMapper.toDescriptor(
                "codes", Types.ARRAY, "array(varchar(64))", false, null, 0);
        assertThat(d.getUserData()).containsEntry(TrinoTypeMapper.OPT_SUBTYPE, "java.lang.String");
    }

    @Test
    void arrayOfBigintMapsToListOfLong() {
        AttributeDescriptor d = TrinoTypeMapper.toDescriptor(
                "counts", Types.ARRAY, "array(bigint)", false, null, 0);
        assertThat(d.getUserData()).containsEntry(TrinoTypeMapper.OPT_SUBTYPE, "java.lang.Long");
    }

    @Test
    void mapOfScalarsMapsToMapWithKeyAndValueClasses() {
        AttributeDescriptor d = TrinoTypeMapper.toDescriptor(
                "source_fields", Types.JAVA_OBJECT, "map(varchar,varchar)", false, null, 0);
        assertThat(d.getType().getBinding()).isEqualTo(java.util.Map.class);
        assertThat(d.getUserData())
                .containsEntry(TrinoTypeMapper.OPT_KEY_CLASS, "java.lang.String")
                .containsEntry(TrinoTypeMapper.OPT_VALUE_CLASS, "java.lang.String");
    }

    @Test
    void mapWithSpacesAndValueParametersStillResolves() {
        AttributeDescriptor d = TrinoTypeMapper.toDescriptor(
                "m", Types.JAVA_OBJECT, "map(varchar, double)", false, null, 0);
        assertThat(d.getType().getBinding()).isEqualTo(java.util.Map.class);
        assertThat(d.getUserData()).containsEntry(TrinoTypeMapper.OPT_VALUE_CLASS, "java.lang.Double");
    }

    @Test
    void arrayOfRowMapsToJsonString() {
        AttributeDescriptor d = TrinoTypeMapper.toDescriptor(
                "identifiers", Types.ARRAY, "array(row(realm varchar,selector varchar))",
                false, null, 0);
        assertThat(d.getType().getBinding()).isEqualTo(String.class);
        assertThat(d.getUserData()).containsEntry(TrinoTypeMapper.OPT_JSON, "true");
    }

    @Test
    void deeplyNestedArrayOfRowStillMapsToJsonString() {
        String t = "array(row(weight double, joint_identities "
                 + "array(row(identifiers array(row(realm varchar, selector varchar))))))";
        AttributeDescriptor d = TrinoTypeMapper.toDescriptor(
                "probabilistic_identities", Types.ARRAY, t, false, null, 0);
        assertThat(d.getType().getBinding()).isEqualTo(String.class);
        assertThat(d.getUserData()).containsEntry(TrinoTypeMapper.OPT_JSON, "true");
    }

    @Test
    void rowMapsToJsonString() {
        AttributeDescriptor d = TrinoTypeMapper.toDescriptor(
                "bbox", Types.JAVA_OBJECT, "row(xmin real,ymin real)", false, null, 0);
        assertThat(d.getType().getBinding()).isEqualTo(String.class);
        assertThat(d.getUserData()).containsEntry(TrinoTypeMapper.OPT_JSON, "true");
    }

    @Test
    void variantMapsToJsonString() {
        AttributeDescriptor d = TrinoTypeMapper.toDescriptor(
                "payload", Types.JAVA_OBJECT, "variant", false, null, 0);
        assertThat(d.getType().getBinding()).isEqualTo(String.class);
        assertThat(d.getUserData()).containsEntry(TrinoTypeMapper.OPT_JSON, "true");
    }

    @Test
    void arrayOfMapFallsBackToJsonRatherThanClaimingAnElementType() {
        // GeoMesa list element types are the primitive set; declaring subtype=Map
        // would promise a shape it cannot serialize, so the column goes to JSON.
        AttributeDescriptor d = TrinoTypeMapper.toDescriptor(
                "a", Types.ARRAY, "array(map(varchar,varchar))", false, null, 0);
        assertThat(d.getType().getBinding()).isEqualTo(String.class);
        assertThat(d.getUserData()).containsEntry(TrinoTypeMapper.OPT_JSON, "true");
    }

    @Test
    void mapWithStructuralValueFallsBackToJson() {
        AttributeDescriptor d = TrinoTypeMapper.toDescriptor(
                "m", Types.JAVA_OBJECT, "map(varchar,row(a int))", false, null, 0);
        assertThat(d.getType().getBinding()).isEqualTo(String.class);
        assertThat(d.getUserData()).containsEntry(TrinoTypeMapper.OPT_JSON, "true");
    }

    @Test
    void decimalKeepsTheOpaqueFallback() {
        // decimal has no ObjectType and is not structural; changing it is a separate
        // judgment call, so the historic behavior is deliberately preserved.
        AttributeDescriptor d = TrinoTypeMapper.toDescriptor(
                "amount", Types.DECIMAL, "decimal(10,2)", false, null, 0);
        assertThat(d.getType().getBinding()).isEqualTo(byte[].class);
    }

    @Test
    void structuralTypeWithoutATypeNameKeepsTheOpaqueFallback() {
        AttributeDescriptor d = TrinoTypeMapper.toDescriptor(
                "unknown", Types.ARRAY, null, false, null, 0);
        assertThat(d.getType().getBinding()).isEqualTo(byte[].class);
    }

    @Test
    void fiveArgOverloadStillCompilesAndDefersToTheOpaqueFallback() {
        AttributeDescriptor d = TrinoTypeMapper.toDescriptor("x", Types.ARRAY, false, null, 0);
        assertThat(d.getType().getBinding()).isEqualTo(byte[].class);
    }
}
