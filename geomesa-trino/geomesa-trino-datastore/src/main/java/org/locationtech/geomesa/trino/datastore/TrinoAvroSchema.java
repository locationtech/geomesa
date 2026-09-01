/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.trino.datastore;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;

import java.io.IOException;
import java.io.StringWriter;
import java.util.List;
import java.util.Locale;
import java.util.Optional;

/**
 * Derives an Avro schema from a Trino structural type signature, for the {@code json-schema}
 * attribute option read by the FileSystem data store.
 *
 * <p>Without this, discovery and storage do not meet: {@link TrinoTypeMapper} marks a nested
 * column {@code json=true} but records nothing about its shape, so an SFT discovered from Trino
 * and then written through the FileSystem store loses the nesting and falls back to a variant.
 *
 * <p>Two properties are deliberate:
 *
 * <ul>
 *   <li><b>Raw input, not {@link TrinoTypeSignature#normalize}.</b> Normalizing lower-cases
 *       quoted {@code row} field names, which was harmless while field names were never
 *       surfaced. They are surfaced now — they become Avro field names, and the JSON keys
 *       matched against them are case-sensitive.</li>
 *   <li><b>All or nothing.</b> Any part that cannot be represented — an anonymous row field,
 *       a non-string map key, an unknown scalar — abandons the whole schema rather than
 *       emitting one that describes the column incorrectly. The column simply stays
 *       {@code json=true} with no declared shape, which is the behavior before this class.</li>
 * </ul>
 */
final class TrinoAvroSchema {

    /** Namespace for generated records. Names are path-derived and unique within a column;
     *  the namespace keeps them from colliding if schemas from several columns are ever
     *  assembled into one document. */
    private static final String NAMESPACE = "org.locationtech.geomesa.trino";

    private static final JsonFactory FACTORY = new JsonFactory();

    /** Raised when some part of the signature has no faithful Avro form; caught in {@link #of}.
     *  Control flow only, so it carries no stack trace. */
    private static final class Unsupported extends RuntimeException {
        Unsupported() {
            super(null, null, false, false);
        }
    }

    private TrinoAvroSchema() {}

    /**
     * The Avro schema for a structural Trino column, or empty when it cannot be derived.
     *
     * @param column column name, used to seed generated record names
     * @param typeName raw (un-normalized) Trino type signature
     */
    static Optional<String> of(String column, String typeName) {
        if (typeName == null) {
            return Optional.empty();
        }
        String t = typeName.trim();
        // the option requires a record, array or map at the top level; a scalar or a
        // schemaless `variant` has nothing to declare
        if (!(startsWith(t, "row(") || startsWith(t, "array(") || startsWith(t, "map("))) {
            return Optional.empty();
        }
        StringWriter out = new StringWriter();
        try (JsonGenerator gen = FACTORY.createGenerator(out)) {
            writeType(gen, t, sanitize(column));
        } catch (Unsupported | IOException e) {
            return Optional.empty();
        }
        return Optional.of(out.toString());
    }

    /**
     * Writes the Avro type for a Trino type.
     *
     * @param path generated record name for this position, extended as we descend
     */
    private static void writeType(JsonGenerator gen, String type, String path) throws IOException {
        String t = type.trim();

        String element = TrinoTypeSignature.arrayElement(t);
        if (element != null) {
            gen.writeStartObject();
            gen.writeStringField("type", "array");
            gen.writeFieldName("items");
            writeNullable(gen, element, path + "_item");
            gen.writeEndObject();
            return;
        }

        String[] kv = TrinoTypeSignature.mapKeyValue(t);
        if (kv != null) {
            String key = baseOf(kv[0]);
            if (!key.equals("varchar") && !key.equals("char")) {
                throw new Unsupported();            // avro map keys are strings
            }
            gen.writeStartObject();
            gen.writeStringField("type", "map");
            gen.writeFieldName("values");
            writeNullable(gen, kv[1], path + "_value");
            gen.writeEndObject();
            return;
        }

        if (startsWith(t, "row(")) {
            List<TrinoTypeSignature.Field> fields = TrinoTypeSignature.rowFields(t);
            if (fields == null) {
                throw new Unsupported();            // anonymous or unparseable fields
            }
            gen.writeStartObject();
            gen.writeStringField("type", "record");
            gen.writeStringField("name", path);
            gen.writeStringField("namespace", NAMESPACE);
            gen.writeArrayFieldStart("fields");
            for (TrinoTypeSignature.Field f : fields) {
                // a name avro cannot carry is not worth approximating - the JSON keys would not match
                if (!isAvroName(f.name())) {
                    throw new Unsupported();
                }
                gen.writeStartObject();
                gen.writeStringField("name", f.name());
                gen.writeFieldName("type");
                writeNullable(gen, f.type(), path + "_" + f.name());
                gen.writeNullField("default");
                gen.writeEndObject();
            }
            gen.writeEndArray();
            gen.writeEndObject();
            return;
        }

        writeScalar(gen, t);
    }

    /** Writes {@code ["null", T]}. Used at field, element and map-value positions — never at the
     *  top level, where the option requires a bare record, array or map. */
    private static void writeNullable(JsonGenerator gen, String type, String path) throws IOException {
        gen.writeStartArray();
        gen.writeString("null");
        writeType(gen, type, path);
        gen.writeEndArray();
    }

    /** Writes the Avro type for a scalar Trino type. */
    private static void writeScalar(JsonGenerator gen, String type) throws IOException {
        String t = type.trim().toLowerCase(Locale.ROOT);
        switch (baseOf(t)) {
            case "varchar", "char" -> gen.writeString("string");
            case "bigint"          -> gen.writeString("long");
            case "integer", "int", "smallint", "tinyint" -> gen.writeString("int");
            case "double"          -> gen.writeString("double");
            case "real"            -> gen.writeString("float");
            case "boolean"         -> gen.writeString("boolean");
            case "varbinary"       -> gen.writeString("bytes");
            case "date"            -> writeLogical(gen, "int", "date");
            case "uuid"            -> writeLogical(gen, "string", "uuid");
            case "time" -> {
                if (t.contains("with time zone")) {
                    throw new Unsupported();        // avro has no time-with-timezone
                }
                writeLogical(gen, "long", "time-micros");
            }
            // both variants are timestamp-micros and the zone is carried by adjust-to-utc, which
            // is what the conversion reads to pick timestamptz over timestamp, and what the
            // parquet layer then sees as isAdjustedToUTC. Note that the seemingly natural
            // spelling of the local variant, local-timestamp-micros, is NOT recognized - it
            // degrades silently to a bare long, losing the type.
            case "timestamp" -> {
                gen.writeStartObject();
                gen.writeStringField("type", "long");
                gen.writeStringField("logicalType", "timestamp-micros");
                gen.writeBooleanField("adjust-to-utc", t.contains("with time zone"));
                gen.writeEndObject();
            }
            case "decimal" -> {
                int[] ps = precisionScale(t);
                if (ps == null) {
                    throw new Unsupported();
                }
                gen.writeStartObject();
                gen.writeStringField("type", "bytes");
                gen.writeStringField("logicalType", "decimal");
                gen.writeNumberField("precision", ps[0]);
                gen.writeNumberField("scale", ps[1]);
                gen.writeEndObject();
            }
            default -> throw new Unsupported();
        }
    }

    private static void writeLogical(JsonGenerator gen, String type, String logicalType)
            throws IOException {
        gen.writeStartObject();
        gen.writeStringField("type", type);
        gen.writeStringField("logicalType", logicalType);
        gen.writeEndObject();
    }

    /** The leading type keyword, stopping at a paren or a space: both {@code timestamp(6) with
     *  time zone} and {@code timestamp with time zone} reduce to {@code timestamp}. */
    private static String baseOf(String type) {
        String t = type.trim().toLowerCase(Locale.ROOT);
        for (int i = 0; i < t.length(); i++) {
            char c = t.charAt(i);
            if (c == '(' || Character.isWhitespace(c)) {
                return t.substring(0, i);
            }
        }
        return t;
    }

    /** Precision and scale of {@code decimal(p,s)}; scale defaults to 0. Null when unparseable. */
    private static int[] precisionScale(String t) {
        int open = t.indexOf('(');
        int close = open < 0 ? -1 : t.indexOf(')', open + 1);
        if (open < 0 || close < 0) {
            return null;
        }
        String[] parts = t.substring(open + 1, close).split(",");
        try {
            int precision = Integer.parseInt(parts[0].trim());
            int scale = parts.length > 1 ? Integer.parseInt(parts[1].trim()) : 0;
            return new int[] { precision, scale };
        } catch (NumberFormatException e) {
            return null;
        }
    }

    /** Whether the keyword at the head of {@code t} is {@code prefix}, ignoring case. */
    private static boolean startsWith(String t, String prefix) {
        return t.regionMatches(true, 0, prefix, 0, prefix.length());
    }

    /** Avro names are {@code [A-Za-z_][A-Za-z0-9_]*} — deliberately ASCII, not
     *  {@link Character#isLetterOrDigit}, which would accept names avro rejects. */
    private static boolean isAvroName(String s) {
        if (s.isEmpty() || !isNameStart(s.charAt(0))) {
            return false;
        }
        for (int i = 1; i < s.length(); i++) {
            char c = s.charAt(i);
            if (!isNameStart(c) && (c < '0' || c > '9')) {
                return false;
            }
        }
        return true;
    }

    private static boolean isNameStart(char c) {
        return (c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z') || c == '_';
    }

    /** Coerces a column name into a valid Avro name, for use as a generated record name. */
    private static String sanitize(String s) {
        StringBuilder sb = new StringBuilder(s.length());
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            sb.append(isNameStart(c) || (c >= '0' && c <= '9') ? c : '_');
        }
        if (sb.length() == 0 || !isNameStart(sb.charAt(0))) {
            sb.insert(0, '_');
        }
        return sb.toString();
    }
}
