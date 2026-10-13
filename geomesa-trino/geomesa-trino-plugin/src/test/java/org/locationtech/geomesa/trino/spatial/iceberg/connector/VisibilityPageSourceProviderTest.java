/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.trino.spatial.iceberg.connector;

import io.airlift.slice.Slices;
import io.trino.plugin.iceberg.*;
import io.trino.spi.Page;
import io.trino.spi.SplitWeight;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.connector.*;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.type.VarcharType;
import org.junit.jupiter.api.Test;

import java.util.*;
import java.util.function.ObjLongConsumer;

import static org.assertj.core.api.Assertions.assertThat;

class VisibilityPageSourceProviderTest {
    private static final IcebergColumnHandle VIS = new IcebergColumnHandle(ColumnIdentity.primitiveColumnIdentity(1, "__vis__"),
        VarcharType.VARCHAR, List.of(), VarcharType.VARCHAR, false, Optional.empty());
    private static final IcebergTableHandle TABLE = new IcebergTableHandle("s", "t", TableType.DATA,
        OptionalLong.empty(), "{}", OptionalInt.empty(), Map.of(), 2, TupleDomain.all(), TupleDomain.all(),
        OptionalLong.empty(), Set.of(), Optional.empty(), "s3://test/t", Map.of(), Optional.empty(), false,
        Optional.empty(), Set.of(), Optional.empty());
    private static IcebergSplit split(TupleDomain<IcebergColumnHandle> statistics) {
        return new IcebergSplit("s3://test/t/f.parquet", 0, 100, 100, 2, IcebergFileFormat.PARQUET,
            0, List.of(), List.of(), SplitWeight.standard(), statistics, Optional.empty(),
            OptionalLong.empty(), OptionalLong.empty(), Optional.empty());
    }
    private static SourcePage page(String... expressions) {
        BlockBuilder b = VarcharType.VARCHAR.createBlockBuilder(null, expressions.length);
        for (String expression : expressions) {
            if (expression == null) b.appendNull(); else VarcharType.VARCHAR.writeString(b, expression);
        }
        return new StubSourcePage(expressions.length, b.build());
    }
    private static final class Delegate implements ConnectorPageSourceProvider {
        List<ColumnHandle> columns;
        ConnectorTableHandle handle;
        SourcePage page;
        @Override public ConnectorPageSource createPageSource(ConnectorTransactionHandle t, ConnectorSession s,
                ConnectorSplit split, ConnectorTableHandle table, Optional<ConnectorTableCredentials> credentials,
                List<ColumnHandle> columns, DynamicFilter dynamicFilter, MemoryContext memoryContext) {
            this.columns = columns;
            this.handle = table;
            return new OneShotPageSource(page);
        }
    }
    private static ConnectorPageSource read(Delegate delegate, ConnectorTableHandle handle,
            TupleDomain<IcebergColumnHandle> stats, List<ColumnHandle> columns) {
        return new VisibilityPageSourceProvider(delegate).createPageSource(null, null, split(stats), handle,
            Optional.empty(), columns, DynamicFilter.EMPTY, null);
    }
    @Test void bypassOmitsUnprojectedVisibilityAndUnwrapsHandle() {
        var delegate = new Delegate();delegate.page = new StubSourcePage(2);
        var source = read(delegate, new VisibilityTableHandle(TABLE, VIS, List.of("A")),
            TupleDomain.withColumnDomains(Map.of(VIS, Domain.singleValue(VarcharType.VARCHAR, Slices.utf8Slice("A")))), List.of());
        assertThat(delegate.handle).isSameAs(TABLE);
        assertThat(delegate.columns).isEmpty();
        assertThat(source.getNextSourcePage().getPositionCount()).isEqualTo(2);
    }
    @Test void projectedVisibilityRemainsAvailableOnBypass() {
        var delegate = new Delegate();delegate.page = page("A", "A");
        var source = read(delegate, new VisibilityTableHandle(TABLE, VIS, List.of("A")),
            TupleDomain.withColumnDomains(Map.of(VIS, Domain.singleValue(VarcharType.VARCHAR, Slices.utf8Slice("A")))), List.of(VIS));
        assertThat(delegate.columns).containsExactly(VIS);
        assertThat(source.getNextSourcePage().getChannelCount()).isEqualTo(1);
    }
    @Test void missingStatsAddsAndStripsVisibilityAndFiltersExactly() {
        var delegate = new Delegate();delegate.page = page("A", "B");
        var source = read(delegate, new VisibilityTableHandle(TABLE, VIS, List.of("A")), TupleDomain.all(), List.of());
        assertThat(delegate.columns).containsExactly(VIS);
        var result = source.getNextSourcePage();
        assertThat(result.getPositionCount()).isEqualTo(1);
        assertThat(result.getChannelCount()).isZero();
    }
    @Test void narrowerAuthPreventsBypassEvenWhenSessionContextWouldPass() {
        var delegate = new Delegate();delegate.page = page("A&B", null);
        var source = read(delegate, new VisibilityTableHandle(TABLE, VIS, List.of("A,B", "A")),
            TupleDomain.withColumnDomains(Map.of(VIS, Domain.singleValue(VarcharType.VARCHAR, Slices.utf8Slice("A&B"), true))), List.of(VIS));
        assertThat(source.getNextSourcePage().getPositionCount()).isEqualTo(1);
        assertThat(source.getMetrics().getMetrics().get("Visibility checked rows").toString()).contains("2");
    }
    @Test void splitManagerKeepsVisibilityProjectedAndRequestsStatsWithBboxDisabled() {
        Connector delegate = new Connector() {
            @Override public void shutdown() {}
            @Override public ConnectorTransactionHandle beginTransaction(io.trino.spi.transaction.IsolationLevel i, boolean r, boolean a) { return null; }
            @Override public ConnectorMetadata getMetadata(ConnectorSession s, ConnectorTransactionHandle t) { return null; }
            @Override public ConnectorSplitManager getSplitManager() {
                return new ConnectorSplitManager() {
                    @Override public ConnectorSplitSource getSplits(ConnectorTransactionHandle t, ConnectorSession s,
                            ConnectorTableHandle h, Set<ColumnHandle> dynamicColumns, Constraint c) {
                        assertThat(h).isInstanceOf(IcebergTableHandle.class);
                        assertThat(((IcebergTableHandle) h).getProjectedColumns()).contains(VIS);
                        assertThat(dynamicColumns).contains(VIS);
                        return null;
                    }
                };
            }
        };
        new SpatialConnector(delegate, null, null, false).getSplitManager()
            .getSplits(null, null, new VisibilityTableHandle(TABLE, VIS, List.of("A")), Set.of(), new Constraint(TupleDomain.all()));
    }
    private static final class StubSourcePage implements SourcePage {
        private Block[] blocks;
        private int positionCount;

        StubSourcePage(int positionCount, Block... blocks) {
            this.positionCount = positionCount;
            this.blocks = blocks;
        }

        @Override public int getPositionCount() { return positionCount; }
        @Override public long getSizeInBytes() { return 0; }
        @Override public long getRetainedSizeInBytes() { return 0; }
        @Override public void retainedBytesForEachPart(ObjLongConsumer<Object> consumer) {}
        @Override public int getChannelCount() { return blocks.length; }
        @Override public Block getBlock(int channel) { return blocks[channel]; }
        @Override public Page getPage() { return new Page(positionCount, blocks); }

        @Override
        public void selectPositions(int[] positions, int offset, int size) {
            Block[] narrowed = new Block[blocks.length];
            for (int i = 0; i < blocks.length; i++) {
                narrowed[i] = blocks[i].getPositions(positions, offset, size);
            }
            blocks = narrowed;
            positionCount = size;
        }
    }

    /** Delegate that yields one page then signals exhaustion. */
    private static final class OneShotPageSource implements ConnectorPageSource {
        private SourcePage page;

        OneShotPageSource(SourcePage page) { this.page = page; }

        @Override public SourcePage getNextSourcePage() {
            SourcePage next = page;
            page = null;
            return next;
        }

        @Override public boolean isFinished() { return page == null; }
        @Override public long getCompletedBytes() { return 0; }
        @Override public long getReadTimeNanos() { return 0; }
        @Override public long getMemoryUsage() { return 0; }
        @Override public OptionalLong getCompletedPositions() { return OptionalLong.empty(); }
        @Override public void close() {}
    }
}
