/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.trino.datastore;

import org.geotools.api.filter.Filter;
import org.geotools.feature.simple.SimpleFeatureBuilder;
import org.geotools.feature.simple.SimpleFeatureTypeBuilder;
import org.geotools.filter.text.ecql.ECQL;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.sql.DriverManager;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** Read-only semantic parity test; supply -Dgeomesa.it.trino.url=jdbc:trino://host:port?user=user. */
@Tag("integration")
class VariantPrefilterIT {
    @Test
    void prefilterAndResidualPreserveGeoToolsEqualityAcrossJsonTypes() throws Exception {
        String url = System.getProperty("geomesa.it.trino.url");
        Assumptions.assumeTrue(url != null, "Supply geomesa.it.trino.url to run VARIANT parity against Trino");
        SimpleFeatureTypeBuilder schema = new SimpleFeatureTypeBuilder();
        schema.setName("variant_parity");
        schema.userData("json", "true");
        schema.add("doc", String.class);
        var sft = schema.buildFeatureType();
        TrinoFilterToSQL translator = new TrinoFilterToSQL();
        translator.setFeatureType(sft);
        List<String> docs = List.of(
            "{\"nested\":{\"key\":\"001\"}}", "{\"nested\":{\"key\":\"1\"}}",
            "{\"nested\":{\"key\":1}}", "{\"nested\":{\"key\":1.0}}",
            "{\"nested\":{\"key\":true}}", "{\"nested\":{\"key\":null}}",
            "{\"nested\":{\"key\":[\"001\",\"other\"]}}", "{\"nested\":{\"key\":{\"x\":1}}}",
            "{}", "{\"nested\":42}", "{\"nested\":null}", "{\"nested\":[1]}",
            "{\"nested\":{\"key\":\"O'Brien\"}}", "{\"nested\":{\"key\":\"TRUE\"}}");
        List<String> tuples = new ArrayList<>();
        for (int i = 0; i < docs.size(); i++) {
            tuples.add("(" + i + ", CAST(JSON '" + docs.get(i).replace("'", "''") + "' AS VARIANT))");
        }
        try (var connection = DriverManager.getConnection(url); var statement = connection.createStatement()) {
            for (String literal : List.of("001", "true", "O'Brien")) {
                Filter filter = ECQL.toFilter("\"$.doc.nested.key\" = '" + literal.replace("'", "''") + "'");
                List<Integer> expected = new ArrayList<>();
                for (int i = 0; i < docs.size(); i++) {
                    if (filter.evaluate(SimpleFeatureBuilder.build(sft, new Object[]{docs.get(i)}, "" + i))) {
                        expected.add(i);
                    }
                }
                List<Integer> candidates = new ArrayList<>();
                try (var results = statement.executeQuery("SELECT id FROM (VALUES " + String.join(",", tuples)
                        + ") AS t(id, doc) WHERE " + translator.variantPrefilter(filter))) {
                    while (results.next()) {
                        candidates.add(results.getInt(1));
                    }
                }
                assertThat(candidates).as("prefilter must not lose matches for %s", literal).containsAll(expected);
                List<Integer> actual = candidates.stream().filter(i -> filter.evaluate(
                    SimpleFeatureBuilder.build(sft, new Object[]{docs.get(i)}, "" + i))).toList();
                assertThat(actual).containsExactlyInAnyOrderElementsOf(expected);
                // A nonmatching string is removed in Trino rather than sent to the worker.
                assertThat(candidates).doesNotContain(1);
            }
        }
    }
}
