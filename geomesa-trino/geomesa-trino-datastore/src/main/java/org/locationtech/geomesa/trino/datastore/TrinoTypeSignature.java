/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.trino.datastore;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Locale;
import java.util.UUID;

/**
 * Trino type-signature grammar.
 *
 */
final class TrinoTypeSignature {

    private TrinoTypeSignature() {}

    /** Lower-cased and trimmed, or "" for null. Trino reports type names lower-case,
     *  but normalizing costs nothing and makes the reader order-independent. It also
     *  lower-cases quoted {@code row} field names, which is harmless: field names are
     *  never surfaced, because any {@code row} becomes {@code json=true}. */
    static String normalize(String typeName) {
        return typeName == null ? "" : typeName.trim().toLowerCase(Locale.ROOT);
    }

    /**
     * The element type of {@code array(...)}, or null when {@code t} is not an array.
     * The text is returned raw; the caller decides whether it is scalar.
     */
    static String arrayElement(String t) {
        return argsOf(t, "array");
    }

    /**
     * The key and value types of {@code map(k,v)}, or null when {@code t} is not a map
     * or its two arguments cannot be separated.
     */
    static String[] mapKeyValue(String t) {
        String args = argsOf(t, "map");
        if (args == null) {
            return null;
        }
        int comma = topLevelComma(args);
        if (comma <= 0 || comma == args.length() - 1) {
            return null;
        }
        return new String[] { args.substring(0, comma), args.substring(comma + 1) };
    }

    /** One field of a {@code row(...)} signature: its declared name and its raw type text. */
    record Field(String name, String type) {}

    /**
     * The fields of {@code row(...)}, or null when {@code t} is not a row, is empty, or has any
     * field this cannot name.
     *
     * <p>The name is the leading token and the type is everything after it. Splitting on the
     * <em>last</em> space would be wrong: a Trino type may itself contain spaces, so
     * {@code first_observed timestamp(6) with time zone} would yield a field named
     * {@code first_observed timestamp(6) with time} of type {@code zone}.
     *
     * <p>Trino also permits anonymous fields ({@code row(varchar, integer)}). Those are rejected
     * rather than given a positional name, because the caller addresses fields by name and an
     * invented one would silently match nothing.
     *
     * <p>Callers that care about the case of quoted identifiers must pass the raw signature:
     * {@link #normalize} lower-cases field names along with everything else.
     */
    static List<Field> rowFields(String t) {
        String args = argsOf(t, "row");
        if (args == null || args.isEmpty()) {
            return null;
        }
        List<Field> fields = new ArrayList<>();
        for (String arg : splitTopLevel(args)) {
            String s = arg.trim();
            if (s.isEmpty()) {
                return null;
            }
            String name;
            String type;
            if (s.charAt(0) == '"') {
                int end = closingQuote(s);
                if (end < 0) {
                    return null;                    // unterminated identifier
                }
                name = s.substring(1, end).replace("\"\"", "\"");
                type = s.substring(end + 1).trim();
            } else {
                int space = firstWhitespace(s);
                if (space < 0) {
                    return null;                    // anonymous field
                }
                name = s.substring(0, space);
                type = s.substring(space + 1).trim();
            }
            if (name.isEmpty() || type.isEmpty()) {
                return null;
            }
            fields.add(new Field(name, type));
        }
        return fields;
    }

    /** Whether {@code t} is a structural type with no flat GeoTools equivalent. */
    static boolean isRowOrVariant(String t) {
        return t.startsWith("row(") || t.equals("variant") || t.startsWith("variant(");
    }

    /** Whether {@code t} names any structural type at all. */
    static boolean isStructural(String t) {
        return t.startsWith("array(") || t.startsWith("map(") || isRowOrVariant(t);
    }

    /**
     * Java binding for a scalar Trino type name, or null when it is structural or is
     * something GeoMesa's {@code ObjectType} cannot carry as a list element or map
     * key/value. Returning null routes the whole column to JSON, which beats declaring
     * an element type GeoMesa could not then serialize.
     *
     * <p>{@code decimal} deliberately returns null, which sends the whole column to JSON.
     * That is better than the alternatives for a nested decimal, because a JSON leaf can be an
     * unquoted number carrying every digit — {@code Double} as a list element would be lossy, and
     * {@code String} would quote a number that is not one. A top-level {@code decimal} has no such
     * option and is mapped to {@code String} instead; see {@link TrinoTypeMapper#toDescriptor}.
     */
    static Class<?> scalarBinding(String typeName) {
        String t = typeName.trim();
        int paren = t.indexOf('(');                 // varchar(32), timestamp(6), decimal(10,2)
        String base = (paren > 0 ? t.substring(0, paren) : t).trim();
        if (base.startsWith("timestamp") || base.startsWith("time ")) {
            return Date.class;                      // incl. "timestamp(6) with time zone"
        }
        return switch (base) {
            case "varchar", "char" -> String.class;
            case "bigint"          -> Long.class;
            case "integer", "int", "smallint", "tinyint" -> Integer.class;
            case "double"          -> Double.class;
            case "real"            -> Float.class;
            case "boolean"         -> Boolean.class;
            case "date", "timestamp", "time" -> Date.class;
            case "uuid"            -> UUID.class;
            case "varbinary"       -> byte[].class;
            default                -> null;
        };
    }

    /** The argument list of {@code <kind>(...)}, or null when {@code t} is not that kind. */
    private static String argsOf(String t, String kind) {
        String prefix = kind + "(";
        // the keyword is matched case-insensitively so that callers needing the raw signature -
        // any that surface field names - are not forced through normalize(), which would
        // lower-case those names too
        if (!t.regionMatches(true, 0, prefix, 0, prefix.length()) || !t.endsWith(")")) {
            return null;                            // includes unterminated input
        }
        return t.substring(prefix.length(), t.length() - 1).trim();
    }

    /** Index of the comma separating a map type's key and value, ignoring nested parens and
     *  anything inside a double-quoted identifier. A doubled quote — SQL's escape for a
     *  quote within a quoted identifier — needs no special case: the two toggles cancel,
     *  so {@code "a""b"} correctly ends outside quotes with nothing counted.
     */
    private static int topLevelComma(String s) {
        int depth = 0;
        boolean inQuotes = false;
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            if (c == '"') inQuotes = !inQuotes;
            else if (inQuotes) continue;          // parens/commas in an identifier are not structure
            else if (c == '(') depth++;
            else if (c == ')') depth--;
            else if (c == ',' && depth == 0) return i;
        }
        return -1;
    }

    /** Splits on every top-level comma, with the same paren and quote rules as
     *  {@link #topLevelComma}. */
    private static List<String> splitTopLevel(String s) {
        List<String> out = new ArrayList<>();
        int depth = 0;
        int start = 0;
        boolean inQuotes = false;
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            if (c == '"') inQuotes = !inQuotes;
            else if (inQuotes) continue;
            else if (c == '(') depth++;
            else if (c == ')') depth--;
            else if (c == ',' && depth == 0) {
                out.add(s.substring(start, i));
                start = i + 1;
            }
        }
        out.add(s.substring(start));
        return out;
    }

    /** Index of the quote closing the identifier starting at index 0, skipping the doubled
     *  quotes that escape one within it. -1 when unterminated. */
    private static int closingQuote(String s) {
        int i = 1;
        while (i < s.length()) {
            if (s.charAt(i) == '"') {
                if (i + 1 < s.length() && s.charAt(i + 1) == '"') {
                    i += 2;                         // escaped quote, keep going
                    continue;
                }
                return i;
            }
            i++;
        }
        return -1;
    }

    private static int firstWhitespace(String s) {
        for (int i = 0; i < s.length(); i++) {
            if (Character.isWhitespace(s.charAt(i))) {
                return i;
            }
        }
        return -1;
    }
}
