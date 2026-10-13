/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.trino.spatial.iceberg.connector;

import io.trino.plugin.iceberg.IcebergTableHandle;
import io.trino.plugin.iceberg.TableType;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.Connector;
import io.trino.spi.connector.ConnectorMetadata;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplitManager;
import io.trino.spi.connector.ConnectorSplitSource;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.Constraint;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.session.PropertyMetadata;
import io.trino.spi.transaction.IsolationLevel;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.OptionalLong;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/** Split wrapping remains active for visibility filtering independently of the bbox setting. */
class SpatialConnectorSplitManagerGatingTest {

    private static final IcebergTableHandle TABLE = new IcebergTableHandle("s", "t", TableType.DATA,
        OptionalLong.empty(), "{}", OptionalInt.empty(), Map.of(), 2, TupleDomain.all(), TupleDomain.all(),
        OptionalLong.empty(), Set.of(), Optional.empty(), "s3://test/t", Map.of(), Optional.empty(), false,
        Optional.empty(), Set.of(), Optional.empty());
    private static final List<PropertyMetadata<?>> SESSION_PROPERTIES = List.of(
        PropertyMetadata.booleanProperty("delegate_property", "Delegate session property", false, false));

    /** Sentinel standing in for Iceberg's real split manager. */
    private static final ConnectorSplitManager DELEGATE_SPLIT_MANAGER = new ConnectorSplitManager() {
        @Override
        public ConnectorSplitSource getSplits(ConnectorTransactionHandle transaction,
                                              ConnectorSession session, ConnectorTableHandle table,
                                              Set<ColumnHandle> dynamicFilterColumns,
                                              Constraint constraint) {
            assertThat(table).isSameAs(TABLE);
            assertThat(dynamicFilterColumns).isEmpty();
            assertThat(constraint.getSummary().isAll()).isTrue();
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
            public List<PropertyMetadata<?>> getSessionProperties() {
                return SESSION_PROPERTIES;
            }

            @Override
            public void shutdown() {}
        };
    }

    @Test
    void bboxDisabledStillWrapsAndPreservesOrdinarySplits() {
        SpatialConnector connector = new SpatialConnector(delegateConnector(), null, null, false);
        ConnectorSplitManager manager = connector.getSplitManager();
        assertThat(manager).isNotSameAs(DELEGATE_SPLIT_MANAGER);
        manager.getSplits(null, null, TABLE, Set.of(), new Constraint(TupleDomain.all()));
    }

    @Test
    void bboxEnabledUnwrapsSpatialTableHandleBeforeDelegating() {
        SpatialConnector connector = new SpatialConnector(delegateConnector(), null, null, true);
        ConnectorSplitManager manager = connector.getSplitManager();
        assertThat(manager).isNotSameAs(DELEGATE_SPLIT_MANAGER);
        ConnectorTableHandle spatial = new SpatialTableHandle(TABLE, null, List.of(), 0, 10, 0, 20);
        manager.getSplits(null, null, spatial, Set.of(), new Constraint(TupleDomain.all()));
    }

    @Test
    void sessionPropertiesDelegateWithoutVisibilitySwitch() {
        SpatialConnector connector = new SpatialConnector(delegateConnector());
        assertThat(connector.getSessionProperties()).isSameAs(SESSION_PROPERTIES);
    }
}
