/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.trino.spatial.iceberg;

import io.airlift.slice.Slice;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.block.BlockBuilderStatus;
import io.trino.spi.block.VariableWidthBlock;
import io.trino.spi.type.AbstractType;
import io.trino.spi.type.TypeSignature;
import org.locationtech.geomesa.trino.spatial.iceberg.connector.SpatialConnectorMetadata;

/**
 * Minimal stand-in for Trino's geospatial {@code Geometry} type, for unit tests.
 *
 * <p>The connector identifies geometry constants by name only
 * ({@code "Geometry".equals(constant.getType().getBaseName())} in
 * {@link SpatialConnectorMetadata#tryExtractEnvelope}), so tests only need a
 * {@link io.trino.spi.type.Type} whose base name is {@code "Geometry"}. We can't
 * use the real {@code io.trino.plugin.geospatial.GeometryType}: the
 * {@code trino-geospatial} plugin artifact is no longer published to Maven
 * Central past Trino 476 (only the {@code trino-geospatial-toolkit} library,
 * which holds {@code JtsGeometrySerde}, still is). This fake carries a Slice
 * value exactly like the real type and reports the same base name.
 */
public final class TestGeometryType extends AbstractType {

    public static final TestGeometryType GEOMETRY = new TestGeometryType();

    private TestGeometryType() {
        super(new TypeSignature("Geometry"), Slice.class, VariableWidthBlock.class);
    }

    @Override
    public String getDisplayName() {
        return "Geometry";
    }

    // The remaining abstract Type methods are never exercised by these unit
    // tests — the constant is only inspected for its base name, never read from
    // or written to a block / flat buffer.
    @Override
    public Object getObjectValue(Block block, int position) {
        throw new UnsupportedOperationException();
    }

    @Override
    public BlockBuilder createBlockBuilder(BlockBuilderStatus status, int expectedEntries, int expectedBytesPerEntry) {
        throw new UnsupportedOperationException();
    }

    @Override
    public BlockBuilder createBlockBuilder(BlockBuilderStatus status, int expectedEntries) {
        throw new UnsupportedOperationException();
    }
    @Override
    public int getFlatFixedSize() {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isFlatVariableWidth() {
        throw new UnsupportedOperationException();
    }

    @Override
    public int getFlatVariableWidthSize(Block block, int position) {
        throw new UnsupportedOperationException();
    }

    @Override
    public int getFlatVariableWidthLength(byte[] fixedSizeSlice, int fixedSizeOffset) {
        throw new UnsupportedOperationException();
    }
}
