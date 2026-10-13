/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.trino.spatial.iceberg.connector;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import org.apache.accumulo.access.AccessExpression;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.ArrayList;
import java.util.Optional;

/** Complete writer proofs. Version 1 supports bypass; version 2 adds normalized AND/OR rollups. */
final class VisibilityFooterMetadata {
    static final String METADATA_KEY = "geomesa.visibility.rollup";
    static final int MAX_BYTES = 64 * 1024;
    static final int MAX_EXPRESSIONS = 1024;
    static final int MAX_DEPTH = 128;

    private VisibilityFooterMetadata() {}

    record Summary(List<Slice> allOf, Optional<Slice> anyOf, long rows) {
        VisibilityFooterReader.Result check(List<Slice> auths) {
            if (auths.isEmpty()) return VisibilityFooterReader.Result.FILTER_REQUIRED;
            if (rows == 0) return VisibilityFooterReader.Result.NONE_VISIBLE;
            // Each predicate's context is independent: failure for any one proves that no row
            // can satisfy their conjunction. Passing each OR does not prove a common visible row.
            if (anyOf.isPresent() && !VisibilityPageSourceProvider.visible(anyOf.get(), auths)) {
                return VisibilityFooterReader.Result.NONE_VISIBLE;
            }
            return allOf.stream().allMatch(expression -> VisibilityPageSourceProvider.visible(expression, auths))
                ? VisibilityFooterReader.Result.ALL_VISIBLE : VisibilityFooterReader.Result.FILTER_REQUIRED;
        }
    }

    static boolean allVisible(String encoded, int fieldId, long fileRows, List<Slice> auths) {
        return !auths.isEmpty() && parse(encoded, fieldId, fileRows)
            .map(summary -> summary.check(auths) == VisibilityFooterReader.Result.ALL_VISIBLE)
            .orElse(false);
    }

    static Optional<Summary> parse(String encoded, int fieldId, long fileRows) {
        if (encoded == null || encoded.length() > MAX_BYTES
                || encoded.getBytes(StandardCharsets.UTF_8).length > MAX_BYTES || fieldId <= 0) return Optional.empty();
        try {
            JsonObject json = JsonParser.parseString(encoded).getAsJsonObject();
            long version = number(json.get("version"));
            if ((version != 1 && version != 2) || number(json.get("fieldId")) != fieldId
                    || !"file".equals(string(json.get("scope")))
                    || number(json.get("rowCount")) != fileRows || fileRows < 0) return Optional.empty();
            if (version == 2) {
                String allOf = string(json.get("allOf"));
                String anyOf = string(json.get("anyOf"));
                if (!withinDepthLimit(allOf) || !withinDepthLimit(anyOf)) return Optional.empty();
                // A malformed OR must not be interpreted as FALSE: that would incorrectly prune
                // files containing visible rows. Reject the whole optional summary instead.
                AccessExpression.validate(allOf);
                AccessExpression.validate(anyOf);
                return Optional.of(new Summary(List.of(Slices.utf8Slice(allOf)),
                    Optional.of(Slices.utf8Slice(anyOf)), fileRows));
            }
            var expressions = json.getAsJsonArray("allOf");
            if (expressions == null || expressions.size() > MAX_EXPRESSIONS) return Optional.empty();
            List<Slice> allOf = new ArrayList<>();
            for (JsonElement expression : expressions) {
                String value = string(expression);
                if (!withinDepthLimit(value)) return Optional.empty();
                allOf.add(Slices.utf8Slice(value));
            }
            return Optional.of(new Summary(List.copyOf(allOf), Optional.empty(), fileRows));
        } catch (RuntimeException e) {
            return Optional.empty();
        } catch (StackOverflowError e) {
            return Optional.empty();
        }
    }

    private static boolean withinDepthLimit(String expression) {
        int depth = 0;
        boolean quoted = false;
        boolean escaped = false;
        for (int i = 0; i < expression.length(); i++) {
            char c = expression.charAt(i);
            if (quoted) {
                if (escaped) escaped = false;
                else if (c == '\\') escaped = true;
                else if (c == '"') quoted = false;
            } else if (c == '"') {
                quoted = true;
            } else if (c == '(') {
                if (++depth > MAX_DEPTH) return false;
            } else if (c == ')') {
                if (--depth < 0) return false;
            }
        }
        return depth == 0 && !quoted;
    }

    private static String string(JsonElement value) {
        if (value == null || !value.isJsonPrimitive() || !value.getAsJsonPrimitive().isString()) {
            throw new IllegalArgumentException("Expected string");
        }
        return value.getAsString();
    }

    private static long number(JsonElement value) {
        if (value == null || !value.isJsonPrimitive() || !value.getAsJsonPrimitive().isNumber()
                || !value.getAsString().matches("[0-9]+")) throw new IllegalArgumentException("Expected nonnegative integer");
        return Long.parseLong(value.getAsString());
    }
}
