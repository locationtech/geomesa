/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.trino.benchmark;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Loads {@link TrinoBenchmarkRunner.DatasetConfig} entries from a JSON file
 * on the classpath. CQL strings may reference named WKT constants from a
 * top-level {@code geometries} block via {@code ${NAME}} substitution — the
 * loader inlines them before constructing each {@link
 * TrinoBenchmarkRunner.FilterSpec}.
 *
 * Format (see {@code benchmark_datasets.json}):
 * <pre>{@code
 * {
 *   "geometries": { "NE_US": "POLYGON ((...))", ... },
 *   "datasets": [
 *     { "table": "...", "label": "...",
 *       "schema": "...",   // optional; overrides the run's --schema default
 *       "filters": [
 *         {"label": "...", "cql": "INTERSECTS(geom, ${NE_US})", "featuresOnly": false}
 *       ]
 *     }
 *   ]
 * }
 * }</pre>
 *
 * Why JSON: this list grew to ~80 entries across 7 datasets, all littered with
 * long WKT polygon literals. Externalizing makes adding a dataset a JSON edit
 * rather than a recompile, and named geometry constants keep the per-filter
 * lines readable.
 */
public final class DatasetConfigLoader {

    private static final Pattern VAR_REF = Pattern.compile("\\$\\{([A-Za-z_][A-Za-z0-9_]*)\\}");

    private DatasetConfigLoader() {}

    /** Load datasets from a classpath JSON resource (e.g. "benchmark_datasets.json"). */
    public static List<TrinoBenchmarkRunner.DatasetConfig> load(String resourcePath) {
        try (InputStream in = DatasetConfigLoader.class.getClassLoader().getResourceAsStream(resourcePath)) {
            if (in == null) {
                throw new IllegalStateException("Resource not found on classpath: " + resourcePath);
            }
            JsonNode root = new ObjectMapper().readTree(in);
            Map<String, String> geometries = parseGeometries(root.path("geometries"));
            return parseDatasets(root.path("datasets"), geometries);
        } catch (IOException e) {
            throw new RuntimeException("Failed to load " + resourcePath, e);
        }
    }

    private static Map<String, String> parseGeometries(JsonNode node) {
        Map<String, String> out = new java.util.LinkedHashMap<>();
        if (node.isMissingNode() || node.isNull()) return out;
        for (Iterator<Map.Entry<String, JsonNode>> it = node.fields(); it.hasNext(); ) {
            Map.Entry<String, JsonNode> e = it.next();
            if (e.getKey().startsWith("_")) continue;  // skip _comment etc.
            out.put(e.getKey(), e.getValue().asText());
        }
        return out;
    }

    private static List<TrinoBenchmarkRunner.DatasetConfig> parseDatasets(
            JsonNode arr, Map<String, String> geometries) {
        if (!arr.isArray()) {
            throw new IllegalStateException("'datasets' must be a JSON array");
        }
        List<TrinoBenchmarkRunner.DatasetConfig> out = new ArrayList<>();
        for (JsonNode d : arr) {
            String table  = d.path("table").asText(null);
            String label  = d.path("label").asText(null);
            String schema = d.hasNonNull("schema") ? d.get("schema").asText() : null;
            if (table == null || label == null) {
                throw new IllegalStateException("Each dataset needs 'table' and 'label': " + d);
            }
            List<TrinoBenchmarkRunner.FilterSpec> filters = new ArrayList<>();
            for (JsonNode f : d.path("filters")) {
                String fLabel = f.path("label").asText(null);
                String fCql   = f.path("cql").asText(null);
                if (fLabel == null || fCql == null) {
                    throw new IllegalStateException("Each filter needs 'label' and 'cql': " + f);
                }
                boolean featuresOnly = f.path("featuresOnly").asBoolean(false);
                filters.add(new TrinoBenchmarkRunner.FilterSpec(
                    fLabel, substitute(fCql, geometries), featuresOnly));
            }
            out.add(new TrinoBenchmarkRunner.DatasetConfig(table, label, filters, schema));
        }
        return out;
    }

    /** Replaces ${NAME} tokens with values from the geometry map. An unknown
     *  reference is a hard error — silent fall-through would surface as a
     *  CQL parse error far from the actual bug. */
    static String substitute(String template, Map<String, String> vars) {
        Matcher m = VAR_REF.matcher(template);
        StringBuilder out = new StringBuilder();
        while (m.find()) {
            String name = m.group(1);
            String val = vars.get(name);
            if (val == null) {
                throw new IllegalStateException(
                    "Unknown geometry reference '${" + name + "}' in: " + template);
            }
            m.appendReplacement(out, Matcher.quoteReplacement(val));
        }
        m.appendTail(out);
        return out.toString();
    }
}
