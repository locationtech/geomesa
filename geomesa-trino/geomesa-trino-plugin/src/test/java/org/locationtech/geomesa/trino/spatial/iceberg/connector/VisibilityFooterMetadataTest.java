/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.trino.spatial.iceberg.connector;

import io.airlift.slice.Slices;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class VisibilityFooterMetadataTest {
    private static final String SUMMARY = """
        {"version":1,"fieldId":2,"scope":"file","rowCount":6,"allOf":["A","A&B","C|D"]}
        """;

    private static boolean visible(String summary, String... auths) {
        return VisibilityFooterMetadata.allVisible(summary, 2, 6,
            java.util.Arrays.stream(auths).map(Slices::utf8Slice).toList());
    }

    private static VisibilityFooterReader.Result decision(String allOf, String anyOf, String... auths) {
        var json = new com.google.gson.JsonObject();
        json.addProperty("version", 2);
        json.addProperty("fieldId", 2);
        json.addProperty("scope", "file");
        json.addProperty("rowCount", 6);
        json.addProperty("allOf", allOf);
        json.addProperty("anyOf", anyOf);
        return VisibilityFooterMetadata.parse(json.toString(), 2, 6).orElseThrow()
            .check(java.util.Arrays.stream(auths).map(Slices::utf8Slice).toList());
    }

    @Test void normalizedOrPrunesOnlyWhenNoRowCanPass() {
        assertThat(decision("A&B", "A|B", "A,B")).isEqualTo(VisibilityFooterReader.Result.ALL_VISIBLE);
        assertThat(decision("A&B", "A|B", "A")).isEqualTo(VisibilityFooterReader.Result.FILTER_REQUIRED);
        assertThat(decision("A&B", "A|B", "C")).isEqualTo(VisibilityFooterReader.Result.NONE_VISIBLE);
        assertThat(decision("A&B", "A|B", "")).isEqualTo(VisibilityFooterReader.Result.NONE_VISIBLE);
        assertThat(decision("A&B", "A|B", "A,B", "C")).isEqualTo(VisibilityFooterReader.Result.NONE_VISIBLE);
    }

    @Test void separateContextsPassingTheOrStillRequireExactFiltering() {
        // One context can access A, the other B. There may be no row that both can access.
        assertThat(decision("A&B", "A|B", "A", "B")).isEqualTo(VisibilityFooterReader.Result.FILTER_REQUIRED);
    }

    @Test void unrestrictedRowsPreventPruning() {
        assertThat(decision("A&B", "", "C")).isEqualTo(VisibilityFooterReader.Result.FILTER_REQUIRED);
        assertThat(decision("", "", "")).isEqualTo(VisibilityFooterReader.Result.ALL_VISIBLE);
    }

    @Test void compoundOrPreservesConjunctionsWithinEachVisibility() {
        assertThat(decision("A&B&C&D", "(A&B)|(C&D)", "A,C"))
            .isEqualTo(VisibilityFooterReader.Result.NONE_VISIBLE);
        assertThat(decision("A&B&C&D", "(A&B)|(C&D)", "A,B"))
            .isEqualTo(VisibilityFooterReader.Result.FILTER_REQUIRED);
    }

    @Test void malformedOrAndUnsupportedShapesCannotSupplyARejectionProof() {
        String v2 = """
            {"version":2,"fieldId":2,"scope":"file","rowCount":6,"allOf":"A&B","anyOf":"A|B"}
            """;
        for (String invalid : List.of(v2.replace("\"A|B\"", "\"A|\""),
                v2.replace("\"A&B\"", "\"A&\""), v2.replace("\"A|B\"", "null"),
                v2.replace("\"A|B\"", "[]"), v2.replace("\"version\":2", "\"version\":3"))) {
            assertThat(VisibilityFooterMetadata.parse(invalid, 2, 6)).isEmpty();
        }
    }

    @Test void rejectsExcessiveStructuralNestingButAcceptsQuotedParentheses() {
        String deep = "(".repeat(10000) + "A" + ")".repeat(10000);
        var json = new com.google.gson.JsonObject();
        json.addProperty("version", 2);
        json.addProperty("fieldId", 2);
        json.addProperty("scope", "file");
        json.addProperty("rowCount", 6);
        json.addProperty("allOf", deep);
        json.addProperty("anyOf", "A");
        assertThat(VisibilityFooterMetadata.parse(json.toString(), 2, 6)).isEmpty();
        json.addProperty("allOf", "A");
        json.addProperty("anyOf", deep);
        assertThat(VisibilityFooterMetadata.parse(json.toString(), 2, 6)).isEmpty();
        String label = "A\"" + "(".repeat(1000) + "A\\";
        String quoted = org.apache.accumulo.access.AccessExpression.quote(label);
        assertThat(decision(quoted, quoted, label)).isEqualTo(VisibilityFooterReader.Result.ALL_VISIBLE);
    }

    @Test void requiresEveryExpressionForEveryContext() {
        assertThat(visible(SUMMARY, "A,B,C")).isTrue();
        assertThat(visible(SUMMARY, "A,B,C", "A,B,D")).isTrue();
        assertThat(visible(SUMMARY, "A,B,C", "A,C")).isFalse();
        assertThat(visible(SUMMARY, "A,B")).isFalse();
        assertThat(visible(SUMMARY)).isFalse();
    }

    @Test void unrestrictedSummaryAcceptsEmptyAuthorizations() {
        assertThat(visible(SUMMARY.replace("[\"A\",\"A&B\",\"C|D\"]", "[]"), "")).isTrue();
    }

    @Test void failsClosedOnMalformedExpressionsAndPreservesWhitespace() {
        assertThat(visible(SUMMARY.replace("\"A&B\"", "\"A&\""), "A,B,C")).isFalse();
        assertThat(visible(SUMMARY.replace("\"A&B\"", "\" A \""), "A,B,C")).isFalse();
    }

    @Test void rejectsIncompleteUnsupportedAndIncorrectlyTypedMetadata() {
        for (String invalid : List.of("{}", "null", "[]", "garbage",
                SUMMARY.replace("\"version\":1", "\"version\":2"),
                SUMMARY.replace("\"fieldId\":2", "\"fieldId\":3"),
                SUMMARY.replace("\"scope\":\"file\"", "\"scope\":\"row-group\""),
                SUMMARY.replace("\"rowCount\":6", "\"rowCount\":5"),
                SUMMARY.replace("\"rowCount\":6", "\"rowCount\":6.5"),
                SUMMARY.replace("\"rowCount\":6", "\"rowCount\":\"6\""),
                SUMMARY.replace("\"rowCount\":6", "\"rowCount\":18446744073709551622"),
                SUMMARY.replace("\"A&B\"", "null"), SUMMARY.replace("\"A&B\"", "true"))) {
            assertThat(visible(invalid, "A,B,C")).as(invalid).isFalse();
        }
        assertThat(visible(null, "A,B,C")).isFalse();
    }

    @Test void rejectsOversizedMetadata() {
        assertThat(visible(" ".repeat(VisibilityFooterMetadata.MAX_BYTES) + SUMMARY, "A,B,C")).isFalse();
        assertThat(visible(SUMMARY.replace("\"A&B\"", "\"\u20ac".repeat(23000) + "\""), "A,B,C")).isFalse();
    }
}
