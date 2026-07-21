/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.trino.spatial.iceberg.connector;
import org.locationtech.geomesa.trino.security.AuthorizationResolver;
import org.locationtech.geomesa.trino.security.VisibilityAccessControl;

import io.trino.spi.connector.*;
import io.trino.spi.function.FunctionProvider;
import io.trino.spi.function.table.ConnectorTableFunction;
import io.trino.spi.procedure.Procedure;
import io.trino.spi.session.PropertyMetadata;
import io.trino.spi.transaction.IsolationLevel;
import org.locationtech.geomesa.trino.spatial.iceberg.GeoMesaColumnCatalog;

import java.util.List;
import java.util.Optional;
import java.util.Set;

/**
 * Wraps Trino's built-in Iceberg connector, overriding only getMetadata() to
 * inject spatial-predicate pushdown ({@link SpatialConnectorMetadata}). All
 * other methods delegate to the underlying connector so Iceberg's session
 * properties, capabilities, procedures, page-source provider, and write
 * support are fully exposed.
 */
public class SpatialConnector implements Connector {

    private final Connector delegate;
    private final GeoMesaColumnCatalog geomCatalog;
    private final ConnectorAccessControl accessControl;
    private final boolean bboxShortCircuit;

    /**
     * Wraps a delegate connector with no Trino-layer visibility enforcement.
     *
     * @param delegate the underlying iceberg connector
     */
    public SpatialConnector(Connector delegate) {
        this(delegate, null, null, true);
    }

    /**
     * Wraps a delegate connector, optionally installing Trino-layer row-visibility enforcement.
     *
     * @param delegate    the underlying iceberg connector
     * @param catalogName the Trino catalog name (for the injected row filter's
     *                    {@code ViewExpression}); may be null when no resolver.
     * @param resolver    identity→auths resolver; when non-null, Trino-layer
     *                    row-visibility enforcement is installed. When null,
     *                    access control delegates to Iceberg (no extra filter).
     */
    public SpatialConnector(Connector delegate, String catalogName, AuthorizationResolver resolver) {
        this(delegate, catalogName, resolver, true);
    }

    /**
     * Wraps a delegate connector, optionally installing Trino-layer row-visibility enforcement
     * and the page-source bbox cheap-reject.
     *
     * @param delegate       the underlying iceberg connector
     * @param catalogName    the Trino catalog name; may be null when no resolver.
     * @param resolver       identity→auths resolver; null disables Trino-layer enforcement.
     * @param bboxShortCircuit when true, wrap the page source with the bbox cheap-reject
     *                       ({@link SpatialPageSourceProvider}).
     */
    public SpatialConnector(Connector delegate, String catalogName, AuthorizationResolver resolver,
                            boolean bboxShortCircuit) {
        this.delegate = delegate;
        this.geomCatalog = new GeoMesaColumnCatalog();
        this.accessControl = resolver == null ? null
            : new VisibilityAccessControl(catalogName, geomCatalog, resolver);
        this.bboxShortCircuit = bboxShortCircuit;
    }

    /**
     * Begins a transaction on the delegate connector.
     *
     * @param isolationLevel the isolation level
     * @param readOnly whether the transaction is read-only
     * @param autoCommit whether the transaction auto-commits
     * @return the delegate transaction handle
     */
    @Override
    public ConnectorTransactionHandle beginTransaction(IsolationLevel isolationLevel,
                                                       boolean readOnly, boolean autoCommit) {
        return delegate.beginTransaction(isolationLevel, readOnly, autoCommit);
    }

    /**
     * Commits the given transaction on the delegate connector.
     *
     * @param transactionHandle the transaction handle
     */
    @Override
    public void commit(ConnectorTransactionHandle transactionHandle) {
        delegate.commit(transactionHandle);
    }

    /**
     * Rolls back the given transaction on the delegate connector.
     *
     * @param transactionHandle the transaction handle
     */
    @Override
    public void rollback(ConnectorTransactionHandle transactionHandle) {
        delegate.rollback(transactionHandle);
    }

    /** Only override: wrap delegate metadata with the spatial pushdown logic.
     *
     * @param session the connector session
     * @param transactionHandle the transaction handle
     * @return the spatial metadata wrapping the delegate metadata
     */
    @Override
    public ConnectorMetadata getMetadata(ConnectorSession session,
                                         ConnectorTransactionHandle transactionHandle) {
        return new SpatialConnectorMetadata(
            delegate.getMetadata(session, transactionHandle),
            geomCatalog,
            bboxShortCircuit
        );
    }

    /**
     * Returns the delegate split manager.
     *
     * @return the delegate split manager
     */
    @Override
    public ConnectorSplitManager getSplitManager() {
        ConnectorSplitManager splitManager = delegate.getSplitManager();
        if (!bboxShortCircuit) {
            return splitManager;
        }
        // BBOX Short-Circuit: unwrap a SpatialTableHandle before Iceberg's split manager sees it;
        // the pushed bbox/z2 domains ride in the unwrapped handle's predicate, so file/manifest
        // pruning is unaffected.
        return new ConnectorSplitManager() {
            @Override
            public ConnectorSplitSource getSplits(ConnectorTransactionHandle transaction,
                                                  ConnectorSession session, ConnectorTableHandle table,
                                                  DynamicFilter dynamicFilter, Constraint constraint) {
                return splitManager.getSplits(transaction, session, SpatialTableHandle.unwrap(table),
                    dynamicFilter, constraint);
            }
        };
    }

    /**
     * Returns the delegate page-source provider.
     *
     * @return the delegate page-source provider
     */
    @Override
    public ConnectorPageSourceProvider getPageSourceProvider() {
        ConnectorPageSourceProvider provider = delegate.getPageSourceProvider();
        return bboxShortCircuit ? new SpatialPageSourceProvider(provider) : provider;
    }

    /**
     * Returns the delegate page-source provider factory.
     *
     * @return the delegate page-source provider factory
     */
    @Override
    public ConnectorPageSourceProviderFactory getPageSourceProviderFactory() {
        ConnectorPageSourceProviderFactory factory = delegate.getPageSourceProviderFactory();
        return bboxShortCircuit
            ? () -> new SpatialPageSourceProvider(factory.createPageSourceProvider())
            : factory;
    }

    /**
     * Returns the delegate record-set provider.
     *
     * @return the delegate record-set provider
     */
    @Override
    public ConnectorRecordSetProvider getRecordSetProvider() {
        return delegate.getRecordSetProvider();
    }

    /**
     * Returns the delegate page-sink provider.
     *
     * @return the delegate page-sink provider
     */
    @Override
    public ConnectorPageSinkProvider getPageSinkProvider() {
        return delegate.getPageSinkProvider();
    }

    /**
     * Returns the delegate index provider.
     *
     * @return the delegate index provider
     */
    @Override
    public ConnectorIndexProvider getIndexProvider() {
        return delegate.getIndexProvider();
    }

    /**
     * Returns the delegate node-partitioning provider.
     *
     * @return the delegate node-partitioning provider
     */
    @Override
    public ConnectorNodePartitioningProvider getNodePartitioningProvider() {
        return delegate.getNodePartitioningProvider();
    }

    /**
     * Returns the delegate system tables.
     *
     * @return the delegate system tables
     */
    @Override
    public Set<SystemTable> getSystemTables() {
        return delegate.getSystemTables();
    }

    /**
     * Returns the delegate function provider.
     *
     * @return the delegate function provider
     */
    @Override
    public Optional<FunctionProvider> getFunctionProvider() {
        return delegate.getFunctionProvider();
    }

    /**
     * Returns the delegate procedures.
     *
     * @return the delegate procedures
     */
    @Override
    public Set<Procedure> getProcedures() {
        return delegate.getProcedures();
    }

    /**
     * Returns the delegate table procedures.
     *
     * @return the delegate table procedures
     */
    @Override
    public Set<TableProcedureMetadata> getTableProcedures() {
        return delegate.getTableProcedures();
    }

    /**
     * Returns the delegate table functions.
     *
     * @return the delegate table functions
     */
    @Override
    public Set<ConnectorTableFunction> getTableFunctions() {
        return delegate.getTableFunctions();
    }

    /**
     * Returns the delegate session properties.
     *
     * @return the delegate session properties
     */
    @Override
    public List<PropertyMetadata<?>> getSessionProperties() {
        return delegate.getSessionProperties();
    }

    /**
     * Returns the delegate schema properties.
     *
     * @return the delegate schema properties
     */
    @Override
    public List<PropertyMetadata<?>> getSchemaProperties() {
        return delegate.getSchemaProperties();
    }

    /**
     * Returns the delegate analyze properties.
     *
     * @return the delegate analyze properties
     */
    @Override
    public List<PropertyMetadata<?>> getAnalyzeProperties() {
        return delegate.getAnalyzeProperties();
    }

    /**
     * Returns the delegate table properties.
     *
     * @return the delegate table properties
     */
    @Override
    public List<PropertyMetadata<?>> getTableProperties() {
        return delegate.getTableProperties();
    }

    /**
     * Returns the delegate view properties.
     *
     * @return the delegate view properties
     */
    @Override
    public List<PropertyMetadata<?>> getViewProperties() {
        return delegate.getViewProperties();
    }

    /**
     * Returns the delegate materialized-view properties.
     *
     * @return the delegate materialized-view properties
     */
    @Override
    public List<PropertyMetadata<?>> getMaterializedViewProperties() {
        return delegate.getMaterializedViewProperties();
    }

    /**
     * Returns the delegate column properties.
     *
     * @return the delegate column properties
     */
    @Override
    public List<PropertyMetadata<?>> getColumnProperties() {
        return delegate.getColumnProperties();
    }

    /** Returns the visibility-enforcing access control when configured;
     *  otherwise delegates to Iceberg (no Trino-layer row filtering).
     *
     * @return the visibility access control, or the delegate access control
     */
    @Override
    public ConnectorAccessControl getAccessControl() {
        return accessControl != null ? accessControl : delegate.getAccessControl();
    }

    /**
     * Returns whether the delegate connector supports single-statement writes only.
     *
     * @return true if the delegate supports single-statement writes only
     */
    @Override
    public boolean isSingleStatementWritesOnly() {
        return delegate.isSingleStatementWritesOnly();
    }

    /**
     * Returns the delegate connector capabilities.
     *
     * @return the delegate connector capabilities
     */
    @Override
    public Set<ConnectorCapabilities> getCapabilities() {
        return delegate.getCapabilities();
    }

    /**
     * Shuts down the delegate connector.
     */
    @Override
    public void shutdown() {
        delegate.shutdown();
    }
}
