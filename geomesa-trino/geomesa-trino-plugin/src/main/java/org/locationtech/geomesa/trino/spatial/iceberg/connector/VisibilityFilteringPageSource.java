/***********************************************************************
 * Copyright (c) 2013-2026 General Atomics Integrated Intelligence, Inc.
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

/** Applies exact visibility filtering when neither the statistics nor the footer proves access to every row. */
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
    private final VisibilityFooterReader.Result footerResult;
    private final long footerBytes;
    private final long footerNanos;
    private long bypassedRows;
    private long checkedRows;
    private long rejectedRows;
    private long evaluatedValues;

    VisibilityFilteringPageSource(ConnectorPageSource delegate, int outputChannels, int visibilityChannel,
            List<Slice> auths, boolean bypass) {
        this(delegate, outputChannels, visibilityChannel, auths, bypass, null, 0, 0);
    }

    VisibilityFilteringPageSource(ConnectorPageSource delegate, int outputChannels, int visibilityChannel,
            List<Slice> auths, boolean bypass, VisibilityFooterReader.Result footerResult,
            long footerBytes, long footerNanos) {
        this.delegate = delegate;
        this.outputChannels = outputChannels;
        this.visibilityChannel = visibilityChannel;
        this.auths = List.copyOf(auths);
        this.bypass = bypass;
        this.footerResult = footerResult;
        this.footerBytes = footerBytes;
        this.footerNanos = footerNanos;
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
        boolean footerBypass = footerResult == VisibilityFooterReader.Result.ALL_VISIBLE;
        return delegate.getMetrics().mergeWith(new Metrics(Map.ofEntries(
            Map.entry("Visibility bypassed rows", new LongCount(bypassedRows)),
            Map.entry("Visibility checked rows", new LongCount(checkedRows)),
            Map.entry("Visibility rejected rows", new LongCount(rejectedRows)),
            Map.entry("Visibility footer bypassed rows", new LongCount(footerBypass ? bypassedRows : 0)),
            Map.entry("Visibility footer bypassed splits", new LongCount(footerBypass ? 1 : 0)),
            Map.entry("Visibility footer pruned splits", new LongCount(footerResult == VisibilityFooterReader.Result.NONE_VISIBLE ? 1 : 0)),
            Map.entry("Visibility footer missing splits", new LongCount(footerResult == VisibilityFooterReader.Result.MISSING ? 1 : 0)),
            Map.entry("Visibility footer invalid splits", new LongCount(footerResult == VisibilityFooterReader.Result.INVALID ? 1 : 0)),
            Map.entry("Visibility footer unavailable splits", new LongCount(footerResult == VisibilityFooterReader.Result.UNAVAILABLE ? 1 : 0)),
            Map.entry("Visibility footer unproven splits", new LongCount(footerResult == VisibilityFooterReader.Result.FILTER_REQUIRED ? 1 : 0)),
            Map.entry("Visibility fallback value evaluations", new LongCount(evaluatedValues)),
            Map.entry("Visibility bypassed splits", new LongCount(bypass ? 1 : 0)),
            Map.entry("Visibility filtered splits", new LongCount(bypass || footerResult == VisibilityFooterReader.Result.NONE_VISIBLE ? 0 : 1)))));
    }
    @Override public long getCompletedBytes() { return footerBytes + delegate.getCompletedBytes(); }
    @Override public OptionalLong getCompletedPositions() { return delegate.getCompletedPositions(); }
    @Override public long getReadTimeNanos() { return footerNanos + delegate.getReadTimeNanos(); }
    @Override public boolean isFinished() { return delegate.isFinished(); }
    @Override public long getMemoryUsage() { return delegate.getMemoryUsage(); }
    @Override public void close() throws IOException { delegate.close(); }
    @Override public CompletableFuture<?> isBlocked() { return delegate.isBlocked(); }
}
