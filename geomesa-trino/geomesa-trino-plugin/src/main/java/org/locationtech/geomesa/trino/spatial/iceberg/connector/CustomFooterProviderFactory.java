/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.trino.spatial.iceberg.connector;

import com.google.inject.Injector;
import io.trino.parquet.cache.ParquetFooterCache;
import io.trino.plugin.base.metrics.FileFormatDataSourceStats;
import io.trino.plugin.hive.orc.OrcReaderConfig;
import io.trino.plugin.hive.parquet.ParquetReaderConfig;
import io.trino.plugin.iceberg.IcebergConfig;
import io.trino.plugin.iceberg.IcebergFileSystemFactory;
import io.trino.plugin.iceberg.IcebergPageSourceProvider;
import io.trino.plugin.iceberg.encryption.EncryptionManagerFactory;
import io.trino.plugin.iceberg.fileio.ForwardingFileIoFactory;
import io.trino.spi.BlocksHashFactory;
import io.trino.spi.connector.ConnectorPageSourceProvider;
import io.trino.spi.type.TypeManager;

import java.util.Optional;

/** Uses the existing Iceberg injector and the same provider dependencies as Trino 483's factory.
 * Only the footer cache is decorated; all data reading and delete processing stay with Iceberg.
 */
final class CustomFooterProviderFactory {
    private final IcebergFileSystemFactory fileSystem;
    private final ForwardingFileIoFactory fileIo;
    private final FileFormatDataSourceStats stats;
    private final OrcReaderConfig orc;
    private final ParquetReaderConfig parquet;
    private final TypeManager types;
    private final ParquetFooterCache cache;
    private final Optional<BlocksHashFactory> blocksHash;
    private final EncryptionManagerFactory encryption;

    CustomFooterProviderFactory(IcebergFileSystemFactory fileSystem, ForwardingFileIoFactory fileIo,
            FileFormatDataSourceStats stats, OrcReaderConfig orc, ParquetReaderConfig parquet,
            TypeManager types, ParquetFooterCache cache, BlocksHashFactory blocksHash,
            IcebergConfig config, EncryptionManagerFactory encryption) {
        this.fileSystem = fileSystem;
        this.fileIo = fileIo;
        this.stats = stats;
        this.orc = orc;
        this.parquet = parquet;
        this.types = types;
        this.cache = cache;
        this.blocksHash = config.isEqualityDeletesBlocksHashEnabled() ? Optional.of(blocksHash) : Optional.empty();
        this.encryption = encryption;
    }

    static CustomFooterProviderFactory from(Injector injector) {
        // Airlift requires explicit bindings. Retrieve Iceberg's existing bindings instead of
        // asking its injector to create an unbound GeoMesa factory.
        var config = injector.getInstance(IcebergConfig.class);
        return new CustomFooterProviderFactory(injector.getInstance(IcebergFileSystemFactory.class),
            injector.getInstance(ForwardingFileIoFactory.class), injector.getInstance(FileFormatDataSourceStats.class),
            injector.getInstance(OrcReaderConfig.class), injector.getInstance(ParquetReaderConfig.class),
            injector.getInstance(TypeManager.class), injector.getInstance(ParquetFooterCache.class),
            config.isEqualityDeletesBlocksHashEnabled() ? injector.getInstance(BlocksHashFactory.class) : null,
            config, injector.getInstance(EncryptionManagerFactory.class));
    }

    ConnectorPageSourceProvider create(boolean bbox) {
        return create(bbox, true);
    }

    ConnectorPageSourceProvider create(boolean bbox, boolean footer) {
        var scopedCache = new ScopedParquetFooterCache(cache);
        var options = parquet.toParquetReaderOptions();
        var iceberg = new IcebergPageSourceProvider(fileSystem, fileIo, stats, orc.toOrcReaderOptions(),
            options, types, scopedCache, blocksHash, encryption);
        return new VisibilityPageSourceProvider(bbox ? new SpatialPageSourceProvider(iceberg) : iceberg,
            footer ? new VisibilityFooterReader(fileSystem, options, stats, scopedCache) : null);
    }
}
