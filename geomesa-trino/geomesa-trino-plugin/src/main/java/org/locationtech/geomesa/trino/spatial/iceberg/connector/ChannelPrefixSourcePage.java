/***********************************************************************
 * Copyright (c) 2013-2026 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.trino.spatial.iceberg.connector;

import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.connector.SourcePage;

import java.util.function.ObjLongConsumer;

/** A {@link SourcePage} view exposing only the first {@code channelCount} channels of a
 *  delegate, hiding columns added by scan filters to the physical read. */
final class ChannelPrefixSourcePage implements SourcePage {
    private final SourcePage delegate;
    private final int channelCount;

    ChannelPrefixSourcePage(SourcePage delegate, int channelCount) {
        this.delegate = delegate;
        this.channelCount = channelCount;
    }

    @Override public int getPositionCount() { return delegate.getPositionCount(); }
    @Override public long getSizeInBytes() { return delegate.getSizeInBytes(); }
    @Override public long getRetainedSizeInBytes() { return delegate.getRetainedSizeInBytes(); }
    @Override public void retainedBytesForEachPart(ObjLongConsumer<Object> consumer) {
        delegate.retainedBytesForEachPart(consumer);
    }
    @Override public int getChannelCount() { return channelCount; }
    @Override public Block getBlock(int channel) {
        if (channel >= channelCount) {
            throw new IndexOutOfBoundsException("channel " + channel + " >= " + channelCount);
        }
        return delegate.getBlock(channel);
    }
    @Override public Page getPage() {
        int[] channels = new int[channelCount];
        for (int i = 0; i < channelCount; i++) {
            channels[i] = i;
        }
        return delegate.getColumns(channels);
    }
    @Override public Page getColumns(int[] channels) { return delegate.getColumns(channels); }
    @Override public void selectPositions(int[] positions, int offset, int size) {
        delegate.selectPositions(positions, offset, size);
    }
}
