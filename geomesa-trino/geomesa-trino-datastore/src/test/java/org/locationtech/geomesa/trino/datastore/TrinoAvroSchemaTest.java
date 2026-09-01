/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.trino.datastore;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.iceberg.avro.AvroSchemaUtil;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The generated schema is only useful if the code that consumes {@code json-schema} accepts it,
 * so most of these assert on the iceberg type it converts to rather than on its text. That
 * conversion mirrors {@code SimpleFeatureIcebergSchema.buildStructuralType} - parse, wrap in a
 * record, convert, take the single field - which is what actually reads the option.
 */
class TrinoAvroSchemaTest {

    /** Mirrors SimpleFeatureIcebergSchema.buildStructuralType, which is package-private to the fs module. */
    private static org.apache.iceberg.types.Type toIceberg(String avsc) {
        Schema parsed = new Schema.Parser().parse(avsc);
        Schema wrapped = SchemaBuilder.record("wrapper").namespace("tmp")
            .fields().name("value").type(parsed).noDefault().endRecord();
        return AvroSchemaUtil.convert(wrapped).asStructType().fields().get(0).type();
    }

    private static String schemaOf(String column, String trinoType) {
        Optional<String> s = TrinoAvroSchema.of(column, trinoType);
        assertThat(s).as("schema for %s", trinoType).isPresent();
        return s.get();
    }

    @Test
    void arrayOfRowConvertsToAListOfStructs() {
        String avsc = schemaOf("identifiers",
            "array(row(realm varchar, selector varchar, security_label varchar))");
        org.apache.iceberg.types.Type t = toIceberg(avsc);
        assertThat(t.isListType()).isTrue();
        org.apache.iceberg.types.Types.StructType s = t.asListType().elementType().asStructType();
        assertThat(s.fields()).extracting(f -> f.name())
            .containsExactly("realm", "selector", "security_label");
        assertThat(s.fields()).allMatch(f -> f.isOptional());
    }

    /**
     * The reason a row field cannot be split on its last space: this type contains two of them.
     * A wrong split names the field "first_observed timestamp(6) with time".
     */
    @Test
    void aTypeContainingSpacesDoesNotBleedIntoTheFieldName() {
        String avsc = schemaOf("parametrics",
            "array(row(freq_min double, emitter_types array(row(realm varchar, selector varchar)), "
            + "observation_count integer, first_observed timestamp(6) with time zone))");
        org.apache.iceberg.types.Types.StructType s =
            toIceberg(avsc).asListType().elementType().asStructType();
        assertThat(s.fields()).extracting(f -> f.name())
            .containsExactly("freq_min", "emitter_types", "observation_count", "first_observed");
        assertThat(s.field("emitter_types").type().isListType()).isTrue();
        // `with time zone` is the utc-adjusted variant
        assertThat(s.field("first_observed").type().toString()).isEqualTo("timestamptz");
    }

    @Test
    void aTimestampWithoutAZoneStaysLocal() {
        String avsc = schemaOf("t", "row(observed timestamp(6))");
        assertThat(toIceberg(avsc).asStructType().field("observed").type().toString())
            .isEqualTo("timestamp");
    }

    /**
     * The regression test for parsing the raw signature: normalize() lower-cases quoted field
     * names, and the JSON keys matched against them are case-sensitive.
     */
    @Test
    void aQuotedFieldNameKeepsItsCase() {
        String avsc = schemaOf("source_fields", "row(\"valueType\" varchar)");
        assertThat(toIceberg(avsc).asStructType().fields()).extracting(f -> f.name())
            .containsExactly("valueType");
    }

    @Test
    void mapValuesAndDecimalsAreCarried() {
        String avsc = schemaOf("m", "map(varchar, row(amount decimal(38,10)))");
        org.apache.iceberg.types.Type t = toIceberg(avsc);
        assertThat(t.isMapType()).isTrue();
        assertThat(t.asMapType().valueType().asStructType().field("amount").type().toString())
            .isEqualTo("decimal(38, 10)");
    }

    @Test
    void nestedRowsBecomeDistinctlyNamedRecords() {
        // the same shape appearing twice must not collide on a generated name
        String avsc = schemaOf("probabilistic_identities",
            "array(row(weight double, joint_identities array(row(realm varchar, selector varchar))))");
        assertThat(toIceberg(avsc).isListType()).isTrue();   // parses at all == no name conflict
    }

    @Test
    void unrepresentableSignaturesDeclareNothing() {
        // a variant has no shape to declare
        assertThat(TrinoAvroSchema.of("v", "variant")).isEmpty();
        // anonymous row fields cannot be addressed by name
        assertThat(TrinoAvroSchema.of("r", "row(varchar, integer)")).isEmpty();
        // avro map keys are strings
        assertThat(TrinoAvroSchema.of("m", "map(integer, varchar)")).isEmpty();
        // a name avro cannot carry is not approximated
        assertThat(TrinoAvroSchema.of("r", "row(\"my field\" varchar)")).isEmpty();
        // an unknown scalar abandons the whole schema rather than describing it wrongly
        assertThat(TrinoAvroSchema.of("r", "row(a varchar, b ipaddress)")).isEmpty();
        // a scalar is not a valid top level for the option
        assertThat(TrinoAvroSchema.of("s", "varchar")).isEmpty();
        assertThat(TrinoAvroSchema.of("s", null)).isEmpty();
    }

    @Test
    void everyGeneratedFieldIsNullable() {
        // trino row fields are all nullable; a required avro field would reject real data
        String avsc = schemaOf("identifiers", "array(row(realm varchar))");
        assertThat(avsc).contains("[\"null\",");
        assertThat(toIceberg(avsc).asListType().isElementOptional()).isTrue();
    }
}
