/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.trino.spatial.iceberg.connector;

import io.airlift.slice.Slice;
import io.trino.plugin.base.metrics.LongCount;
import io.trino.spi.block.Block;
import io.trino.spi.block.DictionaryBlock;
import io.trino.spi.block.RunLengthEncodedBlock;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.SourcePage;
import io.trino.spi.metrics.Metrics;
import io.trino.spi.type.VarcharType;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;
import java.util.concurrent.CompletableFuture;

/** The exact fallback remains mandatory for mixed files, missing statistics, and invalid expressions. */
final class VisibilityFilteringPageSource implements ConnectorPageSource {
    // Keep UNKNOWN at zero so newly allocated decision arrays need no initialization.
    private static final byte DECISION_UNKNOWN = 0;
    private static final byte DECISION_ADMITTED = 1;
    private static final byte DECISION_REJECTED = 2;

    private final ConnectorPageSource delegate;
    private final int outputChannels;
    private final int visibilityChannel;
    private final List<Slice> auths;
    private final boolean bypass;
    private long bypassedRows;
    private long checkedRows;
    private long rejectedRows;
    private long evaluatedValues;

    VisibilityFilteringPageSource(ConnectorPageSource delegate, int outputChannels, int visibilityChannel,
            List<Slice> auths, boolean bypass) {
        this.delegate = delegate;
        this.outputChannels = outputChannels;
        this.visibilityChannel = visibilityChannel;
        this.auths = List.copyOf(auths);
        this.bypass = bypass;
    }

    @Override
    public SourcePage getNextSourcePage() {
        SourcePage page = delegate.getNextSourcePage();
        if (page == null) return null;
        int count = page.getPositionCount();
        if (bypass) {
            bypassedRows += count;
        } else {
            Block values = page.getBlock(visibilityChannel);
            checkedRows += count;
            if (count > 0 && values instanceof RunLengthEncodedBlock) {
                // Retain Trino's constant-block optimization on the exact path. A single decision
                // covers the whole block, even when file statistics are absent or auths reject it.
                if (!visible(values, 0)) {
                    rejectedRows += count;
                    page.selectPositions(new int[0], 0, 0);
                }
            } else if (count > 0) {
                DictionaryBlock dictionary = values instanceof DictionaryBlock d ? d : null;
                // Evaluate only dictionary entries used
                // in this page; unused or malformed entries cannot affect other rows.
                byte[] decisions = dictionary == null ? null : new byte[dictionary.getDictionary().getPositionCount()];
                int[] retained = new int[count];
                int kept = 0;
                for (int position = 0; position < count; position++) {
                    boolean admitted;
                    if (dictionary == null) {
                        admitted = visible(values, position);
                    } else {
                        int id = dictionary.getId(position);
                        if (decisions[id] == DECISION_UNKNOWN) {
                            decisions[id] = visible(dictionary.getDictionary(), id)
                                ? DECISION_ADMITTED : DECISION_REJECTED;
                        }
                        admitted = decisions[id] == DECISION_ADMITTED;
                    }
                    if (admitted) retained[kept++] = position;
                }
                rejectedRows += count - kept;
                if (kept < count) page.selectPositions(retained, 0, kept);
            }
        }
        return page.getChannelCount() > outputChannels
            ? new ChannelPrefixSourcePage(page, outputChannels) : page;
    }

    private boolean visible(Block block, int position) {
        evaluatedValues++;
        Slice expression = block.isNull(position) ? null : VarcharType.VARCHAR.getSlice(block, position);
        return VisibilityPageSourceProvider.visible(expression, auths);
    }

    @Override public Metrics getMetrics() {
        return delegate.getMetrics().mergeWith(new Metrics(Map.of(
            "Visibility bypassed rows", new LongCount(bypassedRows),
            "Visibility checked rows", new LongCount(checkedRows),
            "Visibility rejected rows", new LongCount(rejectedRows),
            "Visibility fallback value evaluations", new LongCount(evaluatedValues),
            "Visibility bypassed splits", new LongCount(bypass ? 1 : 0),
            "Visibility filtered splits", new LongCount(bypass ? 0 : 1))));
    }
    @Override public long getCompletedBytes() { return delegate.getCompletedBytes(); }
    @Override public OptionalLong getCompletedPositions() { return delegate.getCompletedPositions(); }
    @Override public long getReadTimeNanos() { return delegate.getReadTimeNanos(); }
    @Override public boolean isFinished() { return delegate.isFinished(); }
    @Override public long getMemoryUsage() { return delegate.getMemoryUsage(); }
    @Override public void close() throws IOException { delegate.close(); }
    @Override public CompletableFuture<?> isBlocked() { return delegate.isBlocked(); }
}
