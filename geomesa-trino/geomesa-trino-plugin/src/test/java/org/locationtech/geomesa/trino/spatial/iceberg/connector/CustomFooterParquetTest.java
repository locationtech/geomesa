/***********************************************************************
 * Copyright (c) 2013-2026 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.trino.spatial.iceberg.connector;

import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.filesystem.TrinoInput;
import io.trino.filesystem.TrinoInputFile;
import io.trino.filesystem.memory.MemoryFileSystem;
import io.trino.parquet.ParquetReaderOptions;
import io.trino.parquet.cache.ParquetFooterCache;
import io.trino.plugin.base.metrics.FileFormatDataSourceStats;
import io.trino.plugin.hive.orc.OrcReaderConfig;
import io.trino.plugin.hive.orc.OrcWriterConfig;
import io.trino.plugin.hive.parquet.ParquetReaderConfig;
import io.trino.plugin.hive.parquet.ParquetWriterConfig;
import io.trino.plugin.iceberg.*;
import io.trino.plugin.iceberg.encryption.IcebergEncryptionConfig;
import io.trino.plugin.iceberg.encryption.EncryptionManagerFactory;
import io.trino.plugin.iceberg.delete.DeleteFile;
import io.trino.plugin.iceberg.fileio.ForwardingFileIoFactory;
import io.trino.spi.SplitWeight;
import io.trino.spi.connector.*;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.security.ConnectorIdentity;
import io.trino.spi.type.TypeManager;
import io.trino.spi.type.TypeOperators;
import io.trino.spi.type.VarcharType;
import io.trino.spi.type.BigintType;
import io.trino.spi.type.RealType;
import io.trino.spi.type.RowType;
import io.trino.spi.type.VarbinaryType;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.PartitionSpecParser;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SchemaParser;
import org.apache.iceberg.FileContent;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.encryption.PlaintextEncryptionManager;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Proxy;
import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/** Reads a real file produced by VisibilityMetadataTest through Trino's actual Iceberg page source. */
class CustomFooterParquetTest {
    private static final Location FILE = Location.of("memory:///mixed.parquet");
    private static final IcebergColumnHandle VIS = column(2, "__vis__");
    private static final IcebergColumnHandle NAME = column(3, "name");
    private static final Schema SCHEMA = new Schema(
        Types.NestedField.required(1, "__fid__", Types.StringType.get()),
        Types.NestedField.optional(2, "__vis__", Types.StringType.get()),
        Types.NestedField.optional(3, "name", Types.StringType.get()));
    private static final IcebergTableHandle TABLE = new IcebergTableHandle("s", "t", TableType.DATA,
        OptionalLong.empty(), SchemaParser.toJson(SCHEMA), OptionalInt.of(0),
        Map.of(0, PartitionSpecParser.toJson(PartitionSpec.unpartitioned())), 2,
        TupleDomain.all(), TupleDomain.all(), OptionalLong.empty(), Set.of(NAME), Optional.empty(),
        "memory:///", Map.of(), Optional.empty(), false, Optional.empty(), Set.of(), Optional.empty());

    private static IcebergColumnHandle column(int id, String name) {
        return new IcebergColumnHandle(ColumnIdentity.primitiveColumnIdentity(id, name),
            VarcharType.VARCHAR, List.of(), VarcharType.VARCHAR, false, Optional.empty());
    }

    private static ConnectorSession session() {
        return session(new ParquetReaderConfig().setSmallFileThreshold(io.airlift.units.DataSize.ofBytes(0)));
    }

    private static ConnectorSession session(ParquetReaderConfig parquet) {
        var properties = new IcebergSessionProperties(new IcebergConfig(), new IcebergEncryptionConfig(),
            new OrcReaderConfig(), new OrcWriterConfig(), parquet, new ParquetWriterConfig());
        Map<String, Object> defaults = new HashMap<>();
        properties.getSessionProperties().forEach(property -> defaults.put(property.getName(), property.getDefaultValue()));
        return (ConnectorSession) Proxy.newProxyInstance(ConnectorSession.class.getClassLoader(),
            new Class<?>[]{ConnectorSession.class}, (proxy, method, args) -> switch (method.getName()) {
                case "getIdentity" -> ConnectorIdentity.ofUser("test");
                case "getProperty" -> defaults.get((String) args[0]);
                case "getQueryId" -> "footer-test";
                default -> throw new UnsupportedOperationException(method.getName());
            });
    }

    private static TypeManager types() {
        return (TypeManager) Proxy.newProxyInstance(TypeManager.class.getClassLoader(),
            new Class<?>[]{TypeManager.class}, (proxy, method, args) -> switch (method.getName()) {
                case "getTypeOperators" -> new TypeOperators();
                case "getType" -> args[0].toString().equals("bigint") ? BigintType.BIGINT : VarcharType.VARCHAR;
                default -> throw new UnsupportedOperationException(method.getName());
            });
    }

    @Test void readsWriterSummaryAndOmitsVisibilityOnlyForFullyAuthorizedContexts() throws Exception {
        var fs = new MemoryFileSystem();
        byte[] bytes;
        try (var input = getClass().getResourceAsStream("/fixtures/visibility/mixed.parquet")) {
            bytes = Objects.requireNonNull(input).readAllBytes();
        }
        fs.newOutputFile(FILE).createOrOverwrite(bytes);
        byte[] deleteBytes;
        try (var input = getClass().getResourceAsStream("/fixtures/visibility/positions.parquet")) {
            deleteBytes = Objects.requireNonNull(input).readAllBytes();
        }
        var deletePath = Location.of("memory:///positions.parquet");
        fs.newOutputFile(deletePath).createOrOverwrite(deleteBytes);
        var delete = new DeleteFile(FileContent.POSITION_DELETES, deletePath.toString(), FileFormat.PARQUET,
            1, deleteBytes.length, List.of(), OptionalLong.of(0), OptionalLong.of(0), 2,
            OptionalLong.empty(), Optional.empty(), Optional.empty());
        var credentials = Optional.<ConnectorTableCredentials>of(new IcebergTableCredentials(Map.of(), List.of()));
        try (var executor = Executors.newSingleThreadExecutor()) {
            // Match Airlift's production injector policy: no just-in-time bindings.
            var injector = com.google.inject.Guice.createInjector(binder -> {
                binder.requireExplicitBindings();
                binder.bind(IcebergFileSystemFactory.class).toInstance((identity, properties) -> fs);
                binder.bind(ForwardingFileIoFactory.class).toInstance(new ForwardingFileIoFactory(executor));
                binder.bind(FileFormatDataSourceStats.class).toInstance(new FileFormatDataSourceStats());
                binder.bind(OrcReaderConfig.class).toInstance(new OrcReaderConfig());
                binder.bind(ParquetReaderConfig.class).toInstance(new ParquetReaderConfig());
                binder.bind(TypeManager.class).toInstance(types());
                binder.bind(ParquetFooterCache.class).toInstance(ParquetFooterCache.noop());
                binder.bind(IcebergConfig.class).toInstance(new IcebergConfig().setEqualityDeletesBlocksHashEnabled(false));
                binder.bind(EncryptionManagerFactory.class).toInstance((keys, properties) -> PlaintextEncryptionManager.instance());
            });
            var factory = CustomFooterProviderFactory.from(injector);
            for (boolean bbox : List.of(false, true)) {
                for (boolean useFooter : List.of(false, true)) {
                    for (boolean deletes : List.of(false, true)) {
                        var split = new IcebergSplit(FILE.toString(), 0, bytes.length, bytes.length, 6, IcebergFileFormat.PARQUET,
                            0, List.of(), deletes ? List.of(delete) : List.of(), SplitWeight.standard(), TupleDomain.all(), Optional.empty(),
                            OptionalLong.of(1), OptionalLong.empty(), Optional.empty());
                        var provider = factory.create(bbox, useFooter);
                        for (List<String> auths : List.of(List.of("A,B,C"), List.of("A,B,C", "A,C"), List.of(""))) {
                            for (List<ColumnHandle> columns : List.<List<ColumnHandle>>of(List.of(NAME), List.of(NAME, VIS))) {
                                var handle = new VisibilityTableHandle(TABLE, VIS, auths);
                                try (var source = provider.createPageSource(null, session(), split, handle, credentials,
                                        columns, DynamicFilter.EMPTY, MemoryContext.NO_LIMIT)) {
                                    List<String> names = new ArrayList<>();
                                    while (!source.isFinished()) {
                                        var page = source.getNextSourcePage();
                                        if (page == null) continue;
                                        assertThat(page.getChannelCount()).isEqualTo(columns.size());
                                        for (int position = 0; position < page.getPositionCount(); position++) {
                                            names.add(VarcharType.VARCHAR.getSlice(page.getBlock(0), position).toStringUtf8());
                                        }
                                    }
                                    // The empty literal is emitted for an empty authorization set,
                                    // whether resolved from extra credentials or configured mappings.
                                    boolean noAuthorizations = auths.equals(List.of(""));
                                    boolean bypass = auths.size() == 1 && !noAuthorizations;
                                    List<String> expected = noAuthorizations ? List.of("row-4", "row-5") : bypass
                                        ? List.of("row-0", "row-1", "row-2", "row-3", "row-4", "row-5")
                                        : List.of("row-0", "row-2", "row-3", "row-4", "row-5");
                                    assertThat(names).containsExactlyElementsOf(
                                        deletes && !noAuthorizations ? expected.subList(1, expected.size()) : expected);
                                    assertThat(source.getMetrics().getMetrics().get("Visibility footer bypassed splits").toString())
                                        .contains(bypass && useFooter ? "1" : "0");
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    @Test void footerBytesAreReusedByIcebergWithGlobalCachingDisabled() throws Exception {
        var fs = new MemoryFileSystem();
        byte[] bytes;
        try (var input = getClass().getResourceAsStream("/fixtures/visibility/mixed.parquet")) {
            bytes = Objects.requireNonNull(input).readAllBytes();
        }
        fs.newOutputFile(FILE).createOrOverwrite(bytes);
        AtomicInteger reads = new AtomicInteger();
        TrinoFileSystem countedFs = (TrinoFileSystem) Proxy.newProxyInstance(TrinoFileSystem.class.getClassLoader(),
            new Class<?>[]{TrinoFileSystem.class}, (proxy, method, args) -> {
                Object result = method.invoke(fs, args);
                return result instanceof TrinoInputFile input ? countedInput(input, reads) : result;
            });
        var cache = new ScopedParquetFooterCache(ParquetFooterCache.noop());
        var reader = new VisibilityFooterReader((identity, properties) -> countedFs, ParquetReaderOptions.defaultOptions(),
            new FileFormatDataSourceStats(), cache);
        var split = new IcebergSplit(FILE.toString(), 0, bytes.length, bytes.length, 6, IcebergFileFormat.PARQUET,
            0, List.of(), List.of(), SplitWeight.standard(), TupleDomain.all(), Optional.empty(),
            OptionalLong.empty(), OptionalLong.empty(), Optional.empty());
        var key = new io.trino.parquet.cache.ParquetFooterCacheKey(FILE.toString(), bytes.length);
        try (var scope = reader.scope()) {
            assertThat(reader.allVisible(session(), split, new IcebergTableCredentials(Map.of(), List.of()), 2,
                List.of(io.airlift.slice.Slices.utf8Slice("A,B,C")), MemoryContext.NO_LIMIT)).isTrue();
            assertThat(cache.get(key)).isPresent();
            int footerReads = reads.get();
            assertThat(footerReads).isPositive();
            var input = countedFs.newInputFile(FILE, bytes.length);
            var memory = io.trino.memory.context.AggregatedMemoryContext.newAggregatedMemoryContext(MemoryContext.NO_LIMIT);
            try (var source = io.trino.plugin.hive.parquet.ParquetPageSourceFactory.createDataSource(input,
                    OptionalLong.of(bytes.length), ParquetReaderOptions.builder().withSmallFileThreshold(
                        io.airlift.units.DataSize.ofBytes(0)).build(), memory, new FileFormatDataSourceStats())) {
                var footer = io.trino.parquet.reader.MetadataReader.readFooter(source, ParquetReaderOptions.defaultOptions(),
                    Optional.empty(), Optional.empty(), cache, key);
                assertThat(footer.getFileMetaData().getKeyValueMetaData()).containsKey(VisibilityFooterMetadata.METADATA_KEY);
                assertThat(reads.get()).isEqualTo(footerReads);
            } finally {
                memory.close();
            }
        }
        assertThat(cache.get(key)).isEmpty();
    }

    @Test void prunesRestrictedFilesBeforeCreatingADataReader() throws Exception {
        var fs = new MemoryFileSystem();
        byte[] bytes;
        try (var input = getClass().getResourceAsStream("/fixtures/visibility/restricted.parquet")) {
            bytes = Objects.requireNonNull(input).readAllBytes();
        }
        fs.newOutputFile(FILE).createOrOverwrite(bytes);
        var split = new IcebergSplit(FILE.toString(), 0, bytes.length, bytes.length, 4, IcebergFileFormat.PARQUET,
            0, List.of(), List.of(), SplitWeight.standard(), TupleDomain.all(), Optional.empty(),
            OptionalLong.empty(), OptionalLong.empty(), Optional.empty());
        var cache = new ScopedParquetFooterCache(ParquetFooterCache.noop());
        var reader = new VisibilityFooterReader((identity, properties) -> fs, ParquetReaderOptions.defaultOptions(),
            new FileFormatDataSourceStats(), cache);
        ConnectorPageSourceProvider forbidden = new ConnectorPageSourceProvider() {
            @Override public ConnectorPageSource createPageSource(ConnectorTransactionHandle t, ConnectorSession s,
                    ConnectorSplit sp, ConnectorTableHandle h, Optional<ConnectorTableCredentials> c,
                    List<ColumnHandle> cols, DynamicFilter dynamic, MemoryContext memory) {
                throw new AssertionError("Pruned split must not create its data reader");
            }
        };
        for (List<String> auths : List.of(List.of("Z"), List.of(""), List.of("A,B,C", "Z"))) {
            try (var source = new VisibilityPageSourceProvider(forbidden, reader).createPageSource(null, session(),
                    split, new VisibilityTableHandle(TABLE, VIS, auths),
                    Optional.of(new IcebergTableCredentials(Map.of(), List.of())), List.of(NAME),
                    DynamicFilter.EMPTY, MemoryContext.NO_LIMIT)) {
                assertThat(source.isFinished()).isTrue();
                assertThat(source.getCompletedBytes()).isEqualTo(bytes.length);
                assertThat(source.getMetrics().getMetrics().get("Visibility footer pruned splits").toString()).contains("1");
                assertThat(source.getMetrics().getMetrics().get("Visibility checked rows").toString()).contains("0");
            }
        }
        try (var executor = Executors.newSingleThreadExecutor()) {
            var factory = new CustomFooterProviderFactory((identity, properties) -> fs,
                new ForwardingFileIoFactory(executor), new FileFormatDataSourceStats(), new OrcReaderConfig(),
                new ParquetReaderConfig(), types(), ParquetFooterCache.noop(), null,
                new IcebergConfig().setEqualityDeletesBlocksHashEnabled(false),
                (keys, properties) -> PlaintextEncryptionManager.instance());
            for (boolean footer : List.of(false, true)) {
                for (List<String> auths : List.of(List.of("Z"), List.of("A", "C"))) {
                    try (var source = factory.create(false, footer).createPageSource(null, session(), split,
                            new VisibilityTableHandle(TABLE, VIS, auths),
                            Optional.of(new IcebergTableCredentials(Map.of(), List.of())), List.of(NAME),
                            DynamicFilter.EMPTY, MemoryContext.NO_LIMIT)) {
                        int rows = 0;
                        while (!source.isFinished()) {
                            var page = source.getNextSourcePage();
                            if (page != null) rows += page.getPositionCount();
                        }
                        assertThat(rows).isZero();
                        boolean pruned = footer && auths.size() == 1;
                        assertThat(source.getMetrics().getMetrics().get("Visibility footer pruned splits").toString())
                            .contains(pruned ? "1" : "0");
                        assertThat(source.getMetrics().getMetrics().get("Visibility checked rows").toString())
                            .contains(pruned ? "0" : "4");
                    }
                }
            }
        }
    }

    @Test void defaultSmallFileBufferingReadsTheFileOnlyOnce() throws Exception {
        var fs = new MemoryFileSystem();
        byte[] bytes;
        try (var input = getClass().getResourceAsStream("/fixtures/visibility/mixed.parquet")) {
            bytes = Objects.requireNonNull(input).readAllBytes();
        }
        fs.newOutputFile(FILE).createOrOverwrite(bytes);
        var split = new IcebergSplit(FILE.toString(), 0, bytes.length, bytes.length, 6, IcebergFileFormat.PARQUET,
            0, List.of(), List.of(), SplitWeight.standard(), TupleDomain.all(), Optional.empty(),
            OptionalLong.empty(), OptionalLong.empty(), Optional.empty());
        var parquet = new ParquetReaderConfig();
        assertThat(parquet.toParquetReaderOptions().getSmallFileThreshold().toBytes()).isGreaterThan(bytes.length);
        try (var executor = Executors.newSingleThreadExecutor()) {
            var factory = new CustomFooterProviderFactory((identity, properties) -> fs,
                new ForwardingFileIoFactory(executor), new FileFormatDataSourceStats(), new OrcReaderConfig(),
                parquet, types(), ParquetFooterCache.noop(), null,
                new IcebergConfig().setEqualityDeletesBlocksHashEnabled(false),
                (keys, properties) -> PlaintextEncryptionManager.instance());
            for (boolean footer : List.of(false, true)) {
                try (var source = factory.create(false, footer).createPageSource(null, session(parquet), split,
                        new VisibilityTableHandle(TABLE, VIS, List.of("A,C")),
                        Optional.of(new IcebergTableCredentials(Map.of(), List.of())), List.of(NAME),
                        DynamicFilter.EMPTY, MemoryContext.NO_LIMIT)) {
                    int rows = 0;
                    while (!source.isFinished()) {
                        var page = source.getNextSourcePage();
                        if (page != null) {
                            rows += page.getPositionCount();
                            if (page.getPositionCount() > 0) page.getBlock(0);
                        }
                    }
                    assertThat(rows).isEqualTo(5);
                    assertThat(source.getCompletedBytes()).isEqualTo(bytes.length);
                    assertThat(source.getMetrics().getMetrics().get("Visibility checked rows").toString()).contains("6");
                }
            }
        }
    }

    @Test void combinesActualSpatialFilteringWithFooterBypassAndExactVisibility() throws Exception {
        var fs = new MemoryFileSystem();
        byte[] bytes;
        try (var input = getClass().getResourceAsStream("/fixtures/visibility/spatial.parquet")) {
            bytes = Objects.requireNonNull(input).readAllBytes();
        }
        fs.newOutputFile(FILE).createOrOverwrite(bytes);
        var bboxType = RowType.rowType(RowType.field("xmin", RealType.REAL), RowType.field("ymin", RealType.REAL),
            RowType.field("xmax", RealType.REAL), RowType.field("ymax", RealType.REAL));
        var bboxIdentity = new ColumnIdentity(5, "__geom_bbox__", ColumnIdentity.TypeCategory.STRUCT, List.of(
            ColumnIdentity.primitiveColumnIdentity(6, "xmin"), ColumnIdentity.primitiveColumnIdentity(7, "ymin"),
            ColumnIdentity.primitiveColumnIdentity(8, "xmax"), ColumnIdentity.primitiveColumnIdentity(9, "ymax")));
        List<IcebergColumnHandle> leaves = java.util.stream.IntStream.range(6, 10)
            .mapToObj(id -> new IcebergColumnHandle(bboxIdentity, bboxType, List.of(id), RealType.REAL, false, Optional.empty()))
            .toList();
        var geom = new IcebergColumnHandle(ColumnIdentity.primitiveColumnIdentity(4, "geom"), VarbinaryType.VARBINARY,
            List.of(), VarbinaryType.VARBINARY, false, Optional.empty());
        List<Types.NestedField> fields = new ArrayList<>(SCHEMA.columns());
        fields.add(Types.NestedField.optional(4, "geom", Types.BinaryType.get()));
        fields.add(Types.NestedField.optional(5, "__geom_bbox__", Types.StructType.of(
            Types.NestedField.required(6, "xmin", Types.FloatType.get()), Types.NestedField.required(7, "ymin", Types.FloatType.get()),
            Types.NestedField.required(8, "xmax", Types.FloatType.get()), Types.NestedField.required(9, "ymax", Types.FloatType.get()))));
        fields.add(Types.NestedField.optional(10, "__geom_z2__", Types.StringType.get()));
        var spatialSchema = new Schema(fields);
        var table = new IcebergTableHandle("s", "spatial", TableType.DATA, OptionalLong.empty(), SchemaParser.toJson(spatialSchema),
            OptionalInt.of(0), Map.of(0, PartitionSpecParser.toJson(PartitionSpec.unpartitioned())), 2,
            TupleDomain.all(), TupleDomain.all(), OptionalLong.empty(), Set.of(NAME), Optional.empty(), "memory:///",
            Map.of(), Optional.empty(), false, Optional.empty(), Set.of(), Optional.empty());
        var spatial = new SpatialTableHandle(table, geom, leaves, 0, 0, 10, 10);
        var split = new IcebergSplit(FILE.toString(), 0, bytes.length, bytes.length, 6, IcebergFileFormat.PARQUET,
            0, List.of(), List.of(), SplitWeight.standard(), TupleDomain.all(), Optional.empty(),
            OptionalLong.empty(), OptionalLong.empty(), Optional.empty());
        try (var executor = Executors.newSingleThreadExecutor()) {
            var factory = new CustomFooterProviderFactory((identity, properties) -> fs,
                new ForwardingFileIoFactory(executor), new FileFormatDataSourceStats(), new OrcReaderConfig(),
                new ParquetReaderConfig(), types(), ParquetFooterCache.noop(), null,
                new IcebergConfig().setEqualityDeletesBlocksHashEnabled(false),
                (keys, properties) -> PlaintextEncryptionManager.instance());
            for (boolean footer : List.of(false, true)) {
                for (List<String> auths : List.of(List.of("A,B,C"), List.of("A,B,C", "A,C"))) {
                    for (List<ColumnHandle> columns : List.<List<ColumnHandle>>of(List.of(NAME), List.of(NAME, VIS, geom))) {
                        List<String> names = new ArrayList<>();
                        try (var source = factory.create(true, footer).createPageSource(null, session(), split,
                                new VisibilityTableHandle(spatial, VIS, auths),
                                Optional.of(new IcebergTableCredentials(Map.of(), List.of())), columns,
                                DynamicFilter.EMPTY, MemoryContext.NO_LIMIT)) {
                            while (!source.isFinished()) {
                                var page = source.getNextSourcePage();
                                if (page == null) continue;
                                assertThat(page.getChannelCount()).isEqualTo(columns.size());
                                for (int position = 0; position < page.getPositionCount(); position++) {
                                    String name = VarcharType.VARCHAR.getSlice(page.getBlock(0), position).toStringUtf8();
                                    names.add(name);
                                    if (columns.size() == 3) {
                                        String visibility = page.getBlock(1).isNull(position) ? null
                                            : VarcharType.VARCHAR.getSlice(page.getBlock(1), position).toStringUtf8();
                                        assertThat(visibility).isEqualTo(switch (name) {
                                            case "row-1" -> "A&B";
                                            case "row-2" -> "C|D";
                                            case "row-4" -> "";
                                            case "row-5" -> null;
                                            default -> throw new AssertionError(name);
                                        });
                                        var point = new org.locationtech.jts.io.WKBReader()
                                            .read(VarbinaryType.VARBINARY.getSlice(page.getBlock(2), position).getBytes());
                                        double expected = Integer.parseInt(name.substring(4)) == 4 ? 0
                                            : Integer.parseInt(name.substring(4));
                                        assertThat(point.getCoordinate().x).isEqualTo(expected);
                                        assertThat(point.getCoordinate().y).isEqualTo(expected);
                                    }
                                }
                            }
                            assertThat(source.getMetrics().getMetrics().get("Visibility checked rows").toString())
                                .contains(footer && auths.size() == 1 ? "0" : "4");
                        }
                        assertThat(names).containsExactlyElementsOf(auths.size() == 1
                            ? List.of("row-1", "row-2", "row-4", "row-5") : List.of("row-2", "row-4", "row-5"));
                    }
                }
            }
        }
    }

    private static TrinoInputFile countedInput(TrinoInputFile input, AtomicInteger reads) {
        return (TrinoInputFile) Proxy.newProxyInstance(TrinoInputFile.class.getClassLoader(),
            new Class<?>[]{TrinoInputFile.class}, (proxy, method, args) -> {
                Object result = method.invoke(input, args);
                if (!(result instanceof TrinoInput data)) return result;
                return Proxy.newProxyInstance(TrinoInput.class.getClassLoader(), new Class<?>[]{TrinoInput.class},
                    (inputProxy, inputMethod, inputArgs) -> {
                        if (inputMethod.getName().startsWith("read")) reads.incrementAndGet();
                        return inputMethod.invoke(data, inputArgs);
                    });
            });
    }
}
