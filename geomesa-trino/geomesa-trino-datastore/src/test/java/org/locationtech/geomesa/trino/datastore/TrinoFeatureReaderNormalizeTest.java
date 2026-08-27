/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.trino.datastore;

import io.trino.jdbc.Row;
import org.junit.jupiter.api.Test;

import java.sql.Array;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * {@code normalize} turns driver-specific containers into plain JDK types so a
 * {@code json=true} attribute can be serialized and a {@code List}/{@code Map}
 * attribute can be assigned. The shapes here support nested datatypes:
 * {@code array(varchar)}, {@code array(row(...))}, and {@code array(row(...
 * array(row(...))))}, whose row elements arrive as {@link Row}.
 */
@SuppressWarnings("unchecked")
class TrinoFeatureReaderNormalizeTest {

    /** AssertJ's map assertions need concrete key/value types, not wildcards. */
    private static Map<String, Object> asMap(Object o) {
        return (Map<String, Object>) o;
    }

    /** Minimal java.sql.Array stand-in; only getArray() and getBaseTypeName() are exercised. */
    private static Array array(Object... elements) {
        return new StubArray(elements);
    }

    /** As above, but for drivers that hand back something other than an Object[]. */
    private static Array rawArray(Object raw) {
        return new StubArray(raw);
    }

    private static Row row(String n1, Object v1) {
        return Row.builder().addField(n1, v1).build();
    }

    @Test
    void scalarsPassThrough() throws SQLException {
        assertThat(TrinoFeatureReader.normalize("x")).isEqualTo("x");
        assertThat(TrinoFeatureReader.normalize(7)).isEqualTo(7);
        assertThat(TrinoFeatureReader.normalize(null)).isNull();
    }

    @Test
    void sqlArrayBecomesList() throws SQLException {
        Object out = TrinoFeatureReader.normalize(array("a", "b"));
        assertThat(out).isInstanceOf(List.class).isEqualTo(List.of("a", "b"));
    }

    @Test
    void emptyArrayBecomesEmptyList() throws SQLException {
        assertThat(TrinoFeatureReader.normalize(array())).isEqualTo(List.of());
    }

    @Test
    void rowBecomesMapKeyedByFieldName() throws SQLException {
        Object out = TrinoFeatureReader.normalize(
                Row.builder().addField("realm", "MMSI").addField("selector", "123").build());
        assertThat(out).isInstanceOf(Map.class);
        assertThat(asMap(out)).containsEntry("realm", "MMSI").containsEntry("selector", "123");
    }

    @Test
    void anonymousRowFieldsFallBackToTheirOrdinal() throws SQLException {
        Object out = TrinoFeatureReader.normalize(
                Row.builder().addUnnamedField("a").addUnnamedField("b").build());
        assertThat(asMap(out)).containsKeys("field0", "field1");
    }

    @Test
    void arrayOfRowBecomesListOfMap() throws SQLException {
        Object out = TrinoFeatureReader.normalize(
                array(row("path", "/messageHeader"), row("path", "/position")));
        assertThat(out).isInstanceOf(List.class);
        List<?> list = (List<?>) out;
        assertThat(list).hasSize(2);
        assertThat(asMap(list.get(0))).containsEntry("path", "/messageHeader");
    }

    @Test
    void nestingRecursesAllTheWayDown() throws SQLException {
        Object out = TrinoFeatureReader.normalize(array(
                Row.builder()
                   .addField("weight", 0.75)
                   .addField("joint_identities", array(row("realm", "MMSI")))
                   .build()));
        List<?> hypotheses = (List<?>) out;
        Map<String, Object> h = asMap(hypotheses.get(0));
        assertThat(h).containsEntry("weight", 0.75);
        assertThat(h.get("joint_identities")).isInstanceOf(List.class);
        List<?> jis = (List<?>) h.get("joint_identities");
        assertThat(asMap(jis.get(0))).containsEntry("realm", "MMSI");
    }

    @Test
    void mapValuesAreNormalizedToo() throws SQLException {
        Object out = TrinoFeatureReader.normalize(Map.of("k", array("v")));
        assertThat(asMap(out).get("k")).isEqualTo(List.of("v"));
    }

    /**
     * A primitive array is a legal {@code java.sql.Array} payload in general, but {@code TrinoArray} holds an
     * {@code Object[]}, so it cannot arrive from this driver. It is out of contract: logged, not converted.
     */
    @Test
    void primitiveArrayPayloadIsOutOfContractAndPassesThrough() throws SQLException {
        int[] raw = {1, 2, 3};
        assertThat(TrinoFeatureReader.normalize(rawArray(raw))).isSameAs(raw);
    }

    @Test
    void nullArrayPayloadBecomesNull() throws SQLException {
        assertThat(TrinoFeatureReader.normalize(rawArray(null))).isNull();
    }

    /** Nothing sensible to convert, so it is logged and passed through rather than silently emptied. */
    @Test
    void nonArrayPayloadPassesThrough() throws SQLException {
        assertThat(TrinoFeatureReader.normalize(rawArray("not an array"))).isEqualTo("not an array");
    }

    private static final class StubArray implements Array {
        private final Object elements;
        StubArray(Object elements) { this.elements = elements; }
        @Override public Object getArray() { return elements; }
        @Override public String getBaseTypeName() { return "unused"; }
        @Override public int getBaseType() { return 0; }
        @Override public Object getArray(Map<String, Class<?>> m) { return elements; }
        @Override public Object getArray(long index, int count) { return elements; }
        @Override public Object getArray(long i, int c, Map<String, Class<?>> m) { return elements; }
        @Override public java.sql.ResultSet getResultSet() { throw new UnsupportedOperationException(); }
        @Override public java.sql.ResultSet getResultSet(Map<String, Class<?>> m) { throw new UnsupportedOperationException(); }
        @Override public java.sql.ResultSet getResultSet(long i, int c) { throw new UnsupportedOperationException(); }
        @Override public java.sql.ResultSet getResultSet(long i, int c, Map<String, Class<?>> m) { throw new UnsupportedOperationException(); }
        @Override public void free() { }
    }
}
