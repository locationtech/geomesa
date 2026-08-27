/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.trino.datastore;

import java.util.Date;
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
     * <p>{@code decimal} deliberately returns null: there is no {@code ObjectType} for
     * it, {@code Double} would be lossy and {@code String} would lose arithmetic.
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
        if (!t.startsWith(prefix) || !t.endsWith(")")) {
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
}
