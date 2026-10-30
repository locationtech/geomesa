/***********************************************************************
 * Copyright (c) 2013-2026 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.trino.spatial.iceberg.connector;

import io.airlift.slice.Slices;
import io.trino.parquet.cache.ParquetFooterCache;
import io.trino.parquet.cache.ParquetFooterCacheKey;
import org.junit.jupiter.api.Test;

import java.util.concurrent.Executors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ScopedParquetFooterCacheTest {
    private static final ParquetFooterCacheKey KEY = new ParquetFooterCacheKey("memory:///file", 100);

    @Test void reusesBytesWithNoGlobalCacheAndReleasesThemAfterCreation() throws Exception {
        var cache = new ScopedParquetFooterCache(ParquetFooterCache.noop());
        try (var scope = cache.scope()) {
            cache.put(KEY, Slices.utf8Slice("footer"));
            assertThat(cache.get(KEY)).contains(Slices.utf8Slice("footer"));
            assertThat(cache.get(new ParquetFooterCacheKey("memory:///file", 101))).isEmpty();
            assertThatThrownBy(cache::scope).isInstanceOf(IllegalStateException.class);
        }
        assertThat(cache.get(KEY)).isEmpty();
        try (var scope = cache.scope()) {
            assertThat(cache.get(KEY)).isEmpty();
        }
    }

    @Test void isolatesConcurrentCreationAndInvalidatesWithinScope() throws Exception {
        var cache = new ScopedParquetFooterCache(ParquetFooterCache.noop());
        try (var scope = cache.scope(); var executor = Executors.newSingleThreadExecutor()) {
            cache.put(KEY, Slices.utf8Slice("footer"));
            assertThat(executor.submit(() -> {
                try (var otherScope = cache.scope()) { return cache.get(KEY); }
            }).get()).isEmpty();
            cache.invalidate(KEY);
            assertThat(cache.get(KEY)).isEmpty();
        }
    }
}
