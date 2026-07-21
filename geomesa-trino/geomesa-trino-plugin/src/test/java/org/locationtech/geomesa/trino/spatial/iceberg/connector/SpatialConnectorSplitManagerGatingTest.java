/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.trino.spatial.iceberg.connector;

import io.trino.spi.connector.Connector;
import io.trino.spi.connector.ConnectorMetadata;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplitManager;
import io.trino.spi.connector.ConnectorSplitSource;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.Constraint;
import io.trino.spi.connector.DynamicFilter;
import io.trino.spi.transaction.IsolationLevel;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression guard for the split-manager gate. Only bbox-page-filter ON ever wraps a
 * handle in {@link SpatialTableHandle}, so only then may {@link SpatialConnector#getSplitManager()}
 * interpose a wrapper to unwrap it. When the flag is off it MUST hand back Iceberg's own split
 * manager verbatim.  See {@code SpatialConnector.getSplitManager}.
 */
class SpatialConnectorSplitManagerGatingTest {

    /** Sentinel standing in for Iceberg's real split manager, so we can assert identity. */
    private static final ConnectorSplitManager DELEGATE_SPLIT_MANAGER = new ConnectorSplitManager() {
        @Override
        public ConnectorSplitSource getSplits(ConnectorTransactionHandle transaction,
                                              ConnectorSession session, ConnectorTableHandle table,
                                              DynamicFilter dynamicFilter, Constraint constraint) {
            return null;
        }
    };

    private static Connector delegateConnector() {
        return new Connector() {
            @Override
            public ConnectorTransactionHandle beginTransaction(IsolationLevel isolationLevel,
                                                               boolean readOnly, boolean autoCommit) {
                return null;
            }

            @Override
            public ConnectorMetadata getMetadata(ConnectorSession session,
                                                 ConnectorTransactionHandle transactionHandle) {
                return null;
            }

            @Override
            public ConnectorSplitManager getSplitManager() {
                return DELEGATE_SPLIT_MANAGER;
            }

            @Override
            public void shutdown() {}
        };
    }

    @Test
    void flagOffReturnsIcebergSplitManagerVerbatim() {
        SpatialConnector connector = new SpatialConnector(delegateConnector(), null, null, false);
        assertThat(connector.getSplitManager())
            .as("flag off must not interpose any wrapper — Iceberg pruning depends on its own "
                + "split manager being handed back unchanged")
            .isSameAs(DELEGATE_SPLIT_MANAGER);
    }

    @Test
    void flagOnWrapsSoSpatialTableHandleCanBeUnwrapped() {
        SpatialConnector connector = new SpatialConnector(delegateConnector(), null, null, true);
        assertThat(connector.getSplitManager())
            .as("flag on wraps to unwrap the bbox-short-circuit SpatialTableHandle before Iceberg")
            .isNotSameAs(DELEGATE_SPLIT_MANAGER);
    }
}
