/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.trino.datastore;

import org.geotools.api.feature.simple.SimpleFeatureType;
import org.geotools.api.feature.type.AttributeDescriptor;
import org.geotools.feature.simple.SimpleFeatureTypeBuilder;
import org.junit.jupiter.api.Test;
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes;

import java.sql.Types;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The bindings this mapper emits have to be ones GeoMesa itself recognizes, not merely
 * ones GeoTools will accept — {@code AttributeTypeBuilder.setBinding} takes any class,
 * so an unsupported one fails later and elsewhere.
 *
 * <p>Two distinct contracts are involved, and only the first is obvious.
 * {@code AttributeDescriptor.getListType()} resolves the {@code subtype} option with
 * {@code Class.forName}, so any class name works at runtime. But GeoMesa also encodes
 * a feature type to its own spec string — {@code geomesa.sft.spec} is stored as a table
 * property by the ingest tooling — and that path goes through a short-name table
 * ({@code List[UUID]}, not {@code List[java.util.UUID]}). A binding absent from that
 * table survives a read and then fails to round-trip. These tests pin the round-trip.
 */
class TrinoTypeMapperGeoMesaSpecTest {

    /** One-attribute feature type built from the mapper, for round-tripping. */
    private static SimpleFeatureType sftOf(String name, int sqlType, String typeName) {
        AttributeDescriptor d = TrinoTypeMapper.toDescriptor(name, sqlType, typeName, false, null, 0);
        SimpleFeatureTypeBuilder b = new SimpleFeatureTypeBuilder();
        b.setName("t");
        b.add(d);
        return b.buildFeatureType();
    }

    private static SimpleFeatureType roundTrip(SimpleFeatureType sft) {
        String spec = SimpleFeatureTypes.encodeType(sft);
        return SimpleFeatureTypes.createType(sft.getTypeName(), spec);
    }

    @Test
    void listOfStringRoundTripsThroughTheGeoMesaSpec() {
        SimpleFeatureType in = sftOf("ids", Types.ARRAY, "array(varchar)");
        String spec = SimpleFeatureTypes.encodeType(in);
        assertThat(spec).contains("List[String]");
        SimpleFeatureType out = roundTrip(in);
        assertThat(out.getDescriptor("ids").getType().getBinding()).isEqualTo(List.class);
    }

    /** Every element type the mapper can emit must be in GeoMesa's short-name table. */
    @Test
    void everyEmittableListElementTypeRoundTrips() {
        record Case(String trinoType, Class<?> expected, String specToken) {}
        List<Case> cases = List.of(
            new Case("array(varchar)",  String.class,  "List[String]"),
            new Case("array(bigint)",   Long.class,    "List[Long]"),
            new Case("array(integer)",  Integer.class, "List[Integer]"),
            new Case("array(double)",   Double.class,  "List[Double]"),
            new Case("array(real)",     Float.class,   "List[Float]"),
            new Case("array(boolean)",  Boolean.class, "List[Boolean]"),
            new Case("array(uuid)",     UUID.class,    "List[UUID]"),
            new Case("array(date)",     Date.class,    "List[Date]"),
            new Case("array(timestamp(6) with time zone)", Date.class, "List[Date]"),
            new Case("array(varbinary)", byte[].class, "List[Bytes]"));
        for (Case c : cases) {
            SimpleFeatureType in = sftOf("a", Types.ARRAY, c.trinoType());
            assertThat(in.getDescriptor("a").getUserData())
                    .as("subtype for %s", c.trinoType())
                    .containsEntry(TrinoTypeMapper.OPT_SUBTYPE, c.expected().getName());
            String spec = SimpleFeatureTypes.encodeType(in);
            assertThat(spec).as("spec for %s", c.trinoType()).contains(c.specToken());
            SimpleFeatureType out = SimpleFeatureTypes.createType("t", spec);
            assertThat(out.getDescriptor("a").getType().getBinding())
                    .as("binding after round-trip of %s", c.trinoType())
                    .isEqualTo(List.class);
        }
    }

    @Test
    void mapOfScalarsRoundTripsWithBothKeyAndValueTypes() {
        SimpleFeatureType in = sftOf("m", Types.JAVA_OBJECT, "map(varchar,double)");
        String spec = SimpleFeatureTypes.encodeType(in);
        assertThat(spec).contains("Map[String,Double]");
        SimpleFeatureType out = SimpleFeatureTypes.createType("t", spec);
        assertThat(out.getDescriptor("m").getType().getBinding()).isEqualTo(Map.class);
    }

    @Test
    void jsonFlaggedStringRoundTripsAndKeepsTheOption() {
        SimpleFeatureType in = sftOf("identifiers", Types.ARRAY,
                "array(row(realm varchar, selector varchar))");
        String spec = SimpleFeatureTypes.encodeType(in);
        assertThat(spec).contains("String").contains("json=true");
        SimpleFeatureType out = SimpleFeatureTypes.createType("t", spec);
        AttributeDescriptor d = out.getDescriptor("identifiers");
        assertThat(d.getType().getBinding()).isEqualTo(String.class);
        assertThat(d.getUserData()).containsEntry(TrinoTypeMapper.OPT_JSON, "true");
    }

    /**
     * The shape is declared alongside {@code json=true} and survives the spec round-trip, which
     * is the point of deriving it here: an SFT discovered from Trino can be written back out
     * through a store that supports structural fields without losing its nesting.
     *
     * <p>The spec encoder quotes and escapes any value that is not a simple token, so the JSON
     * needs no special handling to survive - but nothing else pins that, so this does.
     */
    @Test
    void aStructuralColumnDeclaresItsShapeAndTheSchemaSurvivesTheSpec() {
        SimpleFeatureType in = sftOf("identifiers", Types.ARRAY,
                "array(row(realm varchar, selector varchar))");
        String schema = (String) in.getDescriptor("identifiers").getUserData()
                .get(TrinoTypeMapper.OPT_JSON_SCHEMA);
        assertThat(schema).isNotNull().contains("\"realm\"").contains("\"selector\"");

        AttributeDescriptor out = roundTrip(in).getDescriptor("identifiers");
        assertThat(out.getUserData()).containsEntry(TrinoTypeMapper.OPT_JSON_SCHEMA, schema);
    }

    /** A column whose shape cannot be derived is left exactly as it was before: json, no schema. */
    @Test
    void aVariantColumnDeclaresNoShape() {
        SimpleFeatureType in = sftOf("v", Types.JAVA_OBJECT, "variant");
        assertThat(in.getDescriptor("v").getUserData())
                .containsEntry(TrinoTypeMapper.OPT_JSON, "true")
                .doesNotContainKey(TrinoTypeMapper.OPT_JSON_SCHEMA);
    }

    /** The remaining opaque fallback is a supported GeoMesa binding too (Bytes). */
    @Test
    void opaqueFallbackAlsoRoundTrips() {
        SimpleFeatureType in = sftOf("unknown", Types.JAVA_OBJECT, "ipaddress");
        assertThat(SimpleFeatureTypes.encodeType(in)).contains("Bytes");
        assertThat(roundTrip(in).getDescriptor("unknown").getType().getBinding())
                .isEqualTo(byte[].class);
    }

    /**
     * A decimal column round-trips as String. BigDecimal is what GeoTools would use here, but it has no
     * {@code ObjectType}, so {@code encodeType} would throw on it — the reason the mapping is String and
     * the reason this test exists.
     */
    @Test
    void decimalRoundTripsAsString() {
        SimpleFeatureType in = sftOf("amount", Types.DECIMAL, "decimal(38,10)");
        assertThat(SimpleFeatureTypes.encodeType(in)).contains("String");
        assertThat(roundTrip(in).getDescriptor("amount").getType().getBinding())
                .isEqualTo(String.class);
    }
}
