/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.trino.spatial.iceberg.connector;

import io.airlift.slice.Slice;
import io.trino.parquet.cache.ParquetFooterCache;
import io.trino.parquet.cache.ParquetFooterCacheKey;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

/** Shares raw footer bytes with Iceberg during synchronous page-source creation, even when
 * the catalog's long-lived footer cache is disabled. Scopes are isolated between threads.
 */
final class ScopedParquetFooterCache implements ParquetFooterCache {
    private final ParquetFooterCache delegate;
    private final ThreadLocal<Map<ParquetFooterCacheKey, Slice>> current = new ThreadLocal<>();

    ScopedParquetFooterCache(ParquetFooterCache delegate) { this.delegate = delegate; }

    AutoCloseableScope scope() {
        if (current.get() != null) throw new IllegalStateException("Nested footer scope");
        current.set(new HashMap<>());
        return current::remove;
    }

    @Override public Optional<Slice> get(ParquetFooterCacheKey key) throws IOException {
        Map<ParquetFooterCacheKey, Slice> scope = current.get();
        if (scope != null && scope.containsKey(key)) return Optional.of(scope.get(key));
        Optional<Slice> cached = delegate.get(key);
        cached.ifPresent(bytes -> { if (scope != null) scope.put(key, bytes); });
        return cached;
    }

    @Override public void put(ParquetFooterCacheKey key, Slice bytes) {
        Map<ParquetFooterCacheKey, Slice> scope = current.get();
        if (scope != null) scope.put(key, bytes);
        delegate.put(key, bytes);
    }

    @Override public void invalidate(ParquetFooterCacheKey key) {
        Map<ParquetFooterCacheKey, Slice> scope = current.get();
        if (scope != null) scope.remove(key);
        delegate.invalidate(key);
    }

    interface AutoCloseableScope extends AutoCloseable {
        @Override void close();
    }
}
