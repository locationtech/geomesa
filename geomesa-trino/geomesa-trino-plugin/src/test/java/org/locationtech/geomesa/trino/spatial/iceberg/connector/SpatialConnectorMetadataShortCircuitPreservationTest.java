/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.trino.spatial.iceberg.connector;

import io.trino.spi.connector.AggregateFunction;
import io.trino.spi.connector.AggregationApplicationResult;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorMetadata;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.LimitApplicationResult;
import io.trino.spi.connector.SortItem;
import io.trino.spi.connector.TopNApplicationResult;
import org.junit.jupiter.api.Test;
import org.locationtech.geomesa.trino.spatial.iceberg.GeoMesaColumnCatalog;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

/** Aggregation, limit and top-N must not run ahead of the authoritative spatial row filter. */
class SpatialConnectorMetadataShortCircuitPreservationTest {

    /** A wrapped handle; the guards test {@code instanceof} before touching any field, so the inner
     *  Iceberg handle can be null. */
    private static final SpatialTableHandle WRAPPED =
        new SpatialTableHandle(null, null, List.of(), 0, 0, 0, 0);

    private final SpatialConnectorMetadata metadata =
        new SpatialConnectorMetadata(new FailIfCalledDelegate(), new GeoMesaColumnCatalog(), true);

    @Test
    void applyLimitDeclinesForWrappedHandle() {
        assertThat(metadata.applyLimit(null, WRAPPED, 10)).isEmpty();
    }

    @Test
    void applyAggregationDeclinesForWrappedHandle() {
        assertThat(metadata.applyAggregation(null, WRAPPED, List.of(), Map.of(), List.of())).isEmpty();
    }

    @Test
    void applyTopNDeclinesForWrappedHandle() {
        assertThat(metadata.applyTopN(null, WRAPPED, 10, List.of(), Map.of())).isEmpty();
    }

    /** Delegate whose push-down callbacks blow up if invoked — proving the guard short-circuited. */
    private static final class FailIfCalledDelegate implements ConnectorMetadata {
        @Override
        public Optional<LimitApplicationResult<ConnectorTableHandle>> applyLimit(
                ConnectorSession session, ConnectorTableHandle handle, long limit) {
            throw new AssertionError("delegate.applyLimit must not be called for a SpatialTableHandle");
        }

        @Override
        public Optional<AggregationApplicationResult<ConnectorTableHandle>> applyAggregation(
                ConnectorSession session, ConnectorTableHandle handle,
                List<AggregateFunction> aggregates, Map<String, ColumnHandle> assignments,
                List<List<ColumnHandle>> groupingSets) {
            throw new AssertionError("delegate.applyAggregation must not be called for a SpatialTableHandle");
        }

        @Override
        public Optional<TopNApplicationResult<ConnectorTableHandle>> applyTopN(
                ConnectorSession session, ConnectorTableHandle handle,
                long topNCount, List<SortItem> sortItems, Map<String, ColumnHandle> assignments) {
            throw new AssertionError("delegate.applyTopN must not be called for a SpatialTableHandle");
        }
    }
}
