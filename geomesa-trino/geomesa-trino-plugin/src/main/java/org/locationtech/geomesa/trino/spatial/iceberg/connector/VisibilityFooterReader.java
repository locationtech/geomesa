/***********************************************************************
 * Copyright (c) 2013-2026 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.trino.spatial.iceberg.connector;

import io.airlift.slice.Slice;
import io.airlift.units.DataSize;
import io.trino.filesystem.Location;
import io.trino.parquet.ParquetReaderOptions;
import io.trino.parquet.ParquetDataSource;
import io.trino.parquet.cache.ParquetFooterCacheKey;
import io.trino.parquet.reader.MetadataReader;
import io.trino.plugin.base.metrics.FileFormatDataSourceStats;
import io.trino.plugin.iceberg.IcebergFileSystemFactory;
import io.trino.plugin.iceberg.IcebergSplit;
import io.trino.plugin.iceberg.IcebergTableCredentials;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.MemoryContext;

import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;

import static io.trino.memory.context.AggregatedMemoryContext.newAggregatedMemoryContext;
import static io.trino.plugin.hive.parquet.ParquetPageSourceFactory.createDataSource;
import static io.trino.plugin.iceberg.IcebergSessionProperties.isUseFileSizeFromMetadata;
import static io.trino.plugin.iceberg.IcebergSessionProperties.getParquetSmallFileThreshold;

final class VisibilityFooterReader {
    enum Result { ALL_VISIBLE, NONE_VISIBLE, FILTER_REQUIRED, MISSING, INVALID, UNAVAILABLE }
    record Inspection(Result result, long bytes, long nanos) {}
    private final IcebergFileSystemFactory fileSystemFactory;
    private final ParquetReaderOptions options;
    private final FileFormatDataSourceStats stats;
    private final ScopedParquetFooterCache cache;

    VisibilityFooterReader(IcebergFileSystemFactory fileSystemFactory, ParquetReaderOptions options,
            FileFormatDataSourceStats stats, ScopedParquetFooterCache cache) {
        this.fileSystemFactory = fileSystemFactory;
        // The preload must not eagerly buffer a small data file just to read its footer.
        this.options = ParquetReaderOptions.builder(options).withSmallFileThreshold(DataSize.ofBytes(0)).build();
        this.stats = stats;
        this.cache = cache;
    }

    ScopedParquetFooterCache.AutoCloseableScope scope() { return cache.scope(); }

    boolean shouldProbe(ConnectorSession session, IcebergSplit split) {
        // Iceberg buffers files at or below this session threshold in full. A separate footer
        // probe would duplicate that I/O, so retain its normal single-read path for those files.
        return split.fileSize() > getParquetSmallFileThreshold(session).toBytes();
    }

    boolean allVisible(ConnectorSession session, IcebergSplit split, IcebergTableCredentials credentials,
            int fieldId, List<Slice> auths, MemoryContext memoryContext) {
        return check(session, split, credentials, fieldId, auths, memoryContext) == Result.ALL_VISIBLE;
    }

    Result check(ConnectorSession session, IcebergSplit split, IcebergTableCredentials credentials,
            int fieldId, List<Slice> auths, MemoryContext memoryContext) {
        return inspect(session, split, credentials, fieldId, auths, memoryContext).result();
    }

    Inspection inspect(ConnectorSession session, IcebergSplit split, IcebergTableCredentials credentials,
            int fieldId, List<Slice> auths, MemoryContext memoryContext) {
        var fs = fileSystemFactory.create(session.getIdentity(), credentials);
        var file = isUseFileSizeFromMetadata(session)
            ? fs.newInputFile(Location.of(split.path()), split.fileSize()) : fs.newInputFile(Location.of(split.path()));
        var memory = newAggregatedMemoryContext(memoryContext);
        ParquetDataSource source = null;
        try {
            source = createDataSource(file, OptionalLong.of(split.fileSize()), options, memory, stats);
            try (var data = source) {
                Result result = readSummary(data, file.location().toString(), split, fieldId, auths);
                return new Inspection(result, data.getReadBytes(), data.getReadTimeNanos());
            }
        } catch (IOException | RuntimeException e) {
            // Optional evidence cannot remove the exact predicate. Iceberg's normal reader still
            // reports unreadable/corrupt data when the fallback creates its page source.
            return new Inspection(Result.UNAVAILABLE, source == null ? 0 : source.getReadBytes(),
                source == null ? 0 : source.getReadTimeNanos());
        } finally {
            memory.close();
        }
    }

    private Result readSummary(ParquetDataSource source, String location, IcebergSplit split,
            int fieldId, List<Slice> auths) throws IOException {
        var footer = MetadataReader.readFooter(source, options, Optional.empty(), Optional.empty(), cache,
            new ParquetFooterCacheKey(location, split.fileSize()));
        long rows = 0;
        for (var block : footer.getBlocks()) rows = Math.addExact(rows, block.rowCount());
        if (rows != split.fileRecordCount()) return Result.INVALID;
        // Validate the physical field ID as well as the footer's declared identity.
        boolean hasField = footer.getFileMetaData().getSchema().getFields().stream()
            .anyMatch(field -> field.getId() != null && field.getId().intValue() == fieldId
                && field.isPrimitive());
        if (!hasField) return Result.INVALID;
        String encoded = footer.getFileMetaData().getKeyValueMetaData().get(VisibilityFooterMetadata.METADATA_KEY);
        if (encoded == null) return Result.MISSING;
        var expressions = VisibilityFooterMetadata.parse(encoded, fieldId, rows);
        if (expressions.isEmpty()) return Result.INVALID;
        return expressions.get().check(auths);
    }
}
