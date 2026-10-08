/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.trino.spatial;

import io.airlift.slice.Slice;
import io.trino.spi.function.ScalarFunction;
import io.trino.spi.function.SqlType;
import io.trino.spi.type.DateTimeEncoding;
import io.trino.spi.type.UuidType;
import org.locationtech.geomesa.filter.interop.FilterFunctions;

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

}
