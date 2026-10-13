/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.trino.spatial.iceberg.connector;

import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.trino.spi.Page;
import io.trino.plugin.base.metrics.LongCount;
import io.trino.spi.block.Block;
import io.trino.spi.block.DictionaryBlock;
import io.trino.spi.block.RunLengthEncodedBlock;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.SourcePage;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.Range;
import io.trino.spi.predicate.ValueSet;
import io.trino.spi.type.VarcharType;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.OptionalLong;
import java.util.function.ObjLongConsumer;

import static org.assertj.core.api.Assertions.assertThat;

class VisibilityFilteringPageSourceTest {
    private static List<Slice> auths(String... strings) {
        return java.util.Arrays.stream(strings).map(Slices::utf8Slice).toList();
    }
    private static Domain singleton(String expression, boolean nulls) {
        return Domain.create(ValueSet.of(VarcharType.VARCHAR, Slices.utf8Slice(expression)), nulls);
    }
    @Test void singletonPermitsBypassWithAndWithoutNulls() {
        for (boolean nulls : List.of(false, true)) {
            assertThat(VisibilityPageSourceProvider.allVisible(singleton("A&B", nulls), auths("A,B"))).isTrue();
            assertThat(VisibilityPageSourceProvider.allVisible(singleton("A&B", nulls), auths("A"))).isFalse();
        }
    }
    @Test void independentAuthContextsPreserveOrSemantics() {
        assertThat(VisibilityPageSourceProvider.allVisible(singleton("A|B", false), auths("A", "B"))).isTrue();
        assertThat(VisibilityPageSourceProvider.allVisible(singleton("A&B", false), auths("A,B", "A"))).isFalse();
    }
    @Test void missingAndMixedBoundsNeverBypass() {
        assertThat(VisibilityPageSourceProvider.allVisible(null, auths("A,B"))).isFalse();
        assertThat(VisibilityPageSourceProvider.allVisible(Domain.all(VarcharType.VARCHAR), auths("A,B"))).isFalse();
        Domain mixed = Domain.create(ValueSet.ofRanges(Range.range(VarcharType.VARCHAR,
            Slices.utf8Slice("A"), true, Slices.utf8Slice("B"), true)), true);
        assertThat(VisibilityPageSourceProvider.allVisible(mixed, auths("A,B"))).isFalse();
    }
    @Test void invalidExpressionsDoNotBypass() {
        assertThat(VisibilityPageSourceProvider.allVisible(singleton("A&", false), auths("A"))).isFalse();
        assertThat(VisibilityPageSourceProvider.allVisible(singleton("A&", false), auths(""))).isFalse();
    }
    @Test void unrestrictedValuesPassEvenWithEmptyAuths() {
        assertThat(VisibilityPageSourceProvider.allVisible(singleton("", true), auths(""))).isTrue();
        assertThat(VisibilityPageSourceProvider.allVisible(Domain.onlyNull(VarcharType.VARCHAR), auths(""))).isTrue();
    }
    @Test void exactFallbackFiltersCompoundInvalidAndUnrestrictedRowsAndStripsAddedColumn() {
        BlockBuilder values = VarcharType.VARCHAR.createBlockBuilder(null, 7);
        for (String value : new String[]{"A", "A&B", "A|B", "A&", "", null, "B"}) {
            if (value == null) values.appendNull(); else VarcharType.VARCHAR.writeString(values, value);
        }
        var source = new VisibilityFilteringPageSource(new OneShotPageSource(new StubSourcePage(7, values.build())),
            0, 0, auths("A,B", "A"), false);
        SourcePage page = source.getNextSourcePage();
        assertThat(page.getPositionCount()).isEqualTo(4);
        assertThat(page.getChannelCount()).isZero();
        assertThat(source.getMetrics().getMetrics().get("Visibility checked rows").toString()).contains("7");
        assertThat(source.getNextSourcePage()).isNull();
    }
    @Test void constantBlocksAreFilteredWithOneDecision() {
        for (String value : new String[]{"A", "B", "A&", null}) {
            BlockBuilder builder = VarcharType.VARCHAR.createBlockBuilder(null, 1);
            if (value == null) builder.appendNull(); else VarcharType.VARCHAR.writeString(builder, value);
            Block repeated = RunLengthEncodedBlock.create(builder.build(), 1000);
            var source = new VisibilityFilteringPageSource(new OneShotPageSource(new StubSourcePage(1000, repeated)),
                1, 0, auths("A"), false);
            assertThat(source.getNextSourcePage().getPositionCount()).isEqualTo(value == null || "A".equals(value) ? 1000 : 0);
            assertThat(source.getMetrics().getMetrics().get("Visibility fallback value evaluations")).isEqualTo(new LongCount(1));
        }
    }
    @Test void dictionaryDecisionsPreserveNullsDuplicatesAndInvalidExpressions() {
        BlockBuilder builder = VarcharType.VARCHAR.createBlockBuilder(null, 4);
        for (String value : new String[]{"A", "B", "A&", null}) {
            if (value == null) builder.appendNull(); else VarcharType.VARCHAR.writeString(builder, value);
        }
        Block dictionary = DictionaryBlock.create(7, builder.build(), new int[]{0, 1, 0, 2, 3, 1, 3});
        var source = new VisibilityFilteringPageSource(new OneShotPageSource(new StubSourcePage(7, dictionary)),
            1, 0, auths("A"), false);
        SourcePage result = source.getNextSourcePage();
        assertThat(result.getPositionCount()).isEqualTo(4);
        assertThat(result.getBlock(0).isNull(2)).isTrue();
        assertThat(result.getBlock(0).isNull(3)).isTrue();
        assertThat(source.getMetrics().getMetrics().get("Visibility fallback value evaluations")).isEqualTo(new LongCount(4));
    }

    @Test void bypassDoesNotMaterializeVisibility() {
        SourcePage page = new StubSourcePage(100) {
            @Override public Block getBlock(int channel) { throw new AssertionError("must remain lazy"); }
        };
        var source = new VisibilityFilteringPageSource(new OneShotPageSource(page), 0, -1, auths("A"), true);
        assertThat(source.getNextSourcePage().getPositionCount()).isEqualTo(100);
        assertThat(source.getMetrics().getMetrics().get("Visibility bypassed rows").toString()).contains("100");
    }
    private static class StubSourcePage implements SourcePage {
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
