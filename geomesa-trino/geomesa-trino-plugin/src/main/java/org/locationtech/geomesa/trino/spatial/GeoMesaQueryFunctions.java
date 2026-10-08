/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.trino.spatial;

import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.trino.spi.function.ScalarFunction;
import io.trino.spi.function.SqlNullable;
import io.trino.spi.function.SqlType;
import io.trino.spi.type.DateTimeEncoding;
import io.trino.spi.type.UuidType;
import org.locationtech.geomesa.filter.interop.FilterFunctions;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.Point;
import org.locationtech.jts.io.ParseException;
import org.locationtech.jts.io.WKBReader;

import java.util.Date;
import java.util.UUID;

/** Adapts native Trino values to the shared GeoMesa filter function implementations. */
public final class GeoMesaQueryFunctions {

    private GeoMesaQueryFunctions() {}

    @ScalarFunction("geomesa_murmur_hash")
    @SqlType("bigint")
    public static long hashString(@SqlType("varchar") Slice value) {
        return FilterFunctions.murmurHash(value.toStringUtf8());
    }

    @ScalarFunction("geomesa_murmur_hash")
    @SqlType("bigint")
    public static long hashBytes(@SqlType("varbinary") Slice value) {
        return FilterFunctions.murmurHash(value.getBytes());
    }

    @ScalarFunction("geomesa_murmur_hash")
    @SqlType("bigint")
    public static long hashLong(@SqlType("bigint") long value) {
        return FilterFunctions.murmurHash(value);
    }

    @ScalarFunction("geomesa_murmur_hash")
    @SqlType("bigint")
    public static long hashDouble(@SqlType("double") double value) {
        return FilterFunctions.murmurHash(value);
    }

    @ScalarFunction("geomesa_murmur_hash")
    @SqlType("bigint")
    public static long hashReal(@SqlType("real") long value) {
        return FilterFunctions.murmurHash(Float.intBitsToFloat((int) value));
    }

    @ScalarFunction("geomesa_murmur_hash")
    @SqlType("bigint")
    public static long hashDate(@SqlType("timestamp(3) with time zone") long value) {
        return FilterFunctions.murmurHash(new Date(DateTimeEncoding.unpackMillisUtc(value)));
    }

    @ScalarFunction("geomesa_murmur_hash")
    @SqlType("bigint")
    public static long hashUuid(@SqlType("uuid") Slice value) {
        return FilterFunctions.murmurHash(UuidType.trinoUuidToJavaUuid(value));
    }

    @ScalarFunction("geomesa_proxy_id")
    @SqlType("bigint")
    public static long proxyId(@SqlType("varchar") Slice value, @SqlType("boolean") boolean uuid) {
        String id = value.toStringUtf8();
        return uuid ? FilterFunctions.proxyId(UUID.fromString(id)) : FilterFunctions.proxyId(id);
    }

    @ScalarFunction("geomesa_z2")
    @SqlType("varchar")
    public static Slice z2(@SqlType("varbinary") Slice value) {
        Point point = (Point) geometry(value);
        return Slices.utf8Slice(FilterFunctions.z2(point));
    }

    @ScalarFunction("geomesa_xz2")
    @SqlNullable
    @SqlType("varchar")
    public static Slice xz2(@SqlType("varbinary") Slice value) {
        String encoded = FilterFunctions.xz2(geometry(value));
        return encoded == null ? null : Slices.utf8Slice(encoded);
    }

    @ScalarFunction("geomesa_convert2viewer")
    @SqlType("varchar")
    public static Slice convert2viewer(@SqlNullable @SqlType("varchar") Slice value,
            @SqlType("varbinary") Slice wkb, @SqlType("bigint") long millis) {
        String id = value == null ? null : value.toStringUtf8();
        Point point = (Point) geometry(wkb);
        return Slices.utf8Slice(FilterFunctions.convert2viewer(id, point, millis));
    }

    private static Geometry geometry(Slice value) {
        try {
            return new WKBReader().read(value.getBytes());
        } catch (ParseException e) {
            throw new IllegalArgumentException("Invalid GeoMesa WKB geometry", e);
        }
    }
}
