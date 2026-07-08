/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.trino.spatial.iceberg.connector;

import io.trino.plugin.iceberg.IcebergMetadata;
import io.trino.spi.connector.ConnectorMetadata;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.Arrays;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Upgrade tripwire: any {@link ConnectorMetadata} method the Iceberg delegate
 * implements but {@link SpatialConnectorMetadata} neither forwards nor allowlists
 * falls through to the SPI default (no-op or NOT_SUPPORTED) with no compile error.
 */
class SpatialConnectorMetadataDelegationTest {

    /**
     * Deliberately not forwarded — the SPI default (NOT_SUPPORTED) keeps the catalog
     * read-only. Rows written through Trino would lack the companion columns the
     * Python pipeline computes, and null companions are silently excluded by the
     * injected bbox domains — a correctness trap. Removing an entry is a conscious
     * decision (DDL forwards should also invalidate the GeoMesaColumnCatalog cache).
     */
    private static final Set<String> INTENTIONALLY_NOT_FORWARDED = Set.of(
        // Writes / DML — would bypass companion-column computation.
        "applyDelete(ConnectorSession,ConnectorTableHandle)",
        "beginCreateTable(ConnectorSession,ConnectorTableMetadata,Optional,RetryMode,boolean)",
        "beginInsert(ConnectorSession,ConnectorTableHandle,List,RetryMode)",
        "beginMerge(ConnectorSession,ConnectorTableHandle,Map,RetryMode)",
        "executeDelete(ConnectorSession,ConnectorTableHandle)",
        "finishCreateTable(ConnectorSession,ConnectorOutputTableHandle,Collection,Collection)",
        "finishInsert(ConnectorSession,ConnectorInsertTableHandle,List,Collection,Collection)",
        "finishMerge(ConnectorSession,ConnectorMergeTableHandle,List,Collection,Collection)",
        "getInsertLayout(ConnectorSession,ConnectorTableHandle)",
        "getInsertWriterScalingOptions(ConnectorSession,ConnectorTableHandle)",
        "getMergeRowIdColumnHandle(ConnectorSession,ConnectorTableHandle)",
        "getNewTableLayout(ConnectorSession,ConnectorTableMetadata)",
        "getNewTableWriterScalingOptions(ConnectorSession,SchemaTableName,Map)",
        "getRowChangeParadigm(ConnectorSession,ConnectorTableHandle)",
        "getSupportedType(ConnectorSession,Map,Type)",
        "getUpdateLayout(ConnectorSession,ConnectorTableHandle)",
        "truncateTable(ConnectorSession,ConnectorTableHandle)",
        // DDL — CREATE TABLE and column-level schema evolution stay blocked: they
        // must go through the ingest pipeline so the companion columns
        // (__X_bbox__/__X_z2__/__X_xz2__/__vis__) stay consistent. Metadata-only
        // DDL (schema/table/view names, comments, properties, ownership) is
        // forwarded instead — see SpatialConnectorMetadata.
        "addColumn(ConnectorSession,ConnectorTableHandle,ColumnMetadata,ColumnPosition)",
        "addField(ConnectorSession,ConnectorTableHandle,List,String,Type,boolean)",
        "applyPartitioning(ConnectorSession,ConnectorTableHandle,Optional,List)",
        "createTable(ConnectorSession,ConnectorTableMetadata,SaveMode)",
        "dropColumn(ConnectorSession,ConnectorTableHandle,ColumnHandle)",
        "dropDefaultValue(ConnectorSession,ConnectorTableHandle,ColumnHandle)",
        "dropField(ConnectorSession,ConnectorTableHandle,ColumnHandle,List)",
        "dropNotNullConstraint(ConnectorSession,ConnectorTableHandle,ColumnHandle)",
        "renameColumn(ConnectorSession,ConnectorTableHandle,ColumnHandle,String)",
        "renameField(ConnectorSession,ConnectorTableHandle,List,String)",
        "setColumnType(ConnectorSession,ConnectorTableHandle,ColumnHandle,Type)",
        "setDefaultValue(ConnectorSession,ConnectorTableHandle,ColumnHandle,String)",
        "setFieldType(ConnectorSession,ConnectorTableHandle,List,Type)",
        // Materialized views — refresh inserts rows without companions, so the
        // whole MV lifecycle stays blocked. Regular (non-materialized) views are
        // metadata-only and ARE forwarded (see SpatialConnectorMetadata).
        "beginRefreshMaterializedView(ConnectorSession,ConnectorTableHandle,List,boolean,RetryMode,RefreshType)",
        "createMaterializedView(ConnectorSession,SchemaTableName,ConnectorMaterializedViewDefinition,Map,boolean,boolean)",
        "delegateMaterializedViewRefreshToConnector(ConnectorSession,SchemaTableName)",
        "dropMaterializedView(ConnectorSession,SchemaTableName)",
        "finishRefreshMaterializedView(ConnectorSession,ConnectorTableHandle,ConnectorInsertTableHandle,Collection,Collection,List,boolean,boolean,boolean)",
        "getMaterializedViewFreshness(ConnectorSession,SchemaTableName,boolean)",
        "getMaterializedViewProperties(ConnectorSession,SchemaTableName,ConnectorMaterializedViewDefinition)",
        "renameMaterializedView(ConnectorSession,SchemaTableName,SchemaTableName)",
        "setMaterializedViewColumnComment(ConnectorSession,SchemaTableName,String,Optional)",
        // ALTER TABLE EXECUTE — maintenance runs via make / iceberg-spatial-tools.
        "beginTableExecute(ConnectorSession,ConnectorTableExecuteHandle,ConnectorTableHandle)",
        "executeTableExecute(ConnectorSession,ConnectorTableExecuteHandle)",
        "finishTableExecute(ConnectorSession,ConnectorTableExecuteHandle,Collection,List)",
        "getColumnHandlesForTableExecute(ConnectorSession,ConnectorTableHandle,ConnectorTableExecuteHandle)",
        "getLayoutForTableExecute(ConnectorSession,ConnectorTableExecuteHandle)",
        "getTableHandleForExecute(ConnectorSession,ConnectorAccessControl,ConnectorTableHandle,String,Map,RetryMode)",
        // ANALYZE — writes Puffin stats files; same argument.
        "beginStatisticsCollection(ConnectorSession,ConnectorTableHandle)",
        "finishStatisticsCollection(ConnectorSession,ConnectorTableHandle,Collection)",
        "getStatisticsCollectionMetadata(ConnectorSession,ConnectorTableHandle,Map)",
        "getStatisticsCollectionMetadataForWrite(ConnectorSession,ConnectorTableMetadata,boolean)",
        // Misc non-read-path surface, unused by this deployment so far.
        "getFunctionDependencies(ConnectorSession,FunctionId,BoundSignature)",
        "getFunctionMetadata(ConnectorSession,FunctionId)",
        "getFunctions(ConnectorSession,SchemaFunctionName)",
        "getMetrics(ConnectorSession)",
        "getTableCredentials(ConnectorSession,ConnectorTableFunctionHandle)",
        "getTableCredentials(ConnectorSession,ConnectorTableHandle)",
        "getTableCredentials(ConnectorSession,ConnectorWritableTableHandle)",
        "listFunctions(ConnectorSession,String)");

    @Test
    void everyIcebergMetadataMethodIsForwardedOrAllowlisted() {
        Set<String> interfaceSigs = signatures(ConnectorMetadata.class.getMethods());
        // Only the delegate's SPI overrides matter — not Iceberg-internal public helpers.
        Set<String> icebergSigs = signatures(IcebergMetadata.class.getDeclaredMethods());
        icebergSigs.retainAll(interfaceSigs);
        Set<String> spatialSigs = signatures(SpatialConnectorMetadata.class.getDeclaredMethods());

        Set<String> missing = new TreeSet<>(icebergSigs);
        missing.removeAll(spatialSigs);
        missing.removeAll(INTENTIONALLY_NOT_FORWARDED);
        assertThat(missing)
            .as("ConnectorMetadata methods the Iceberg delegate implements but "
                + "SpatialConnectorMetadata neither forwards nor allowlists. Forward them "
                + "(plain delegation) or add them to INTENTIONALLY_NOT_FORWARDED with a reason.")
            .isEmpty();

        // Keep the allowlist honest: entries must still exist on the delegate.
        Set<String> stale = new TreeSet<>(INTENTIONALLY_NOT_FORWARDED);
        stale.removeAll(icebergSigs);
        assertThat(stale)
            .as("Allowlist entries IcebergMetadata no longer implements — remove them.")
            .isEmpty();
    }

    private static Set<String> signatures(Method[] methods) {
        return Arrays.stream(methods)
            .filter(m -> !m.isSynthetic() && !m.isBridge())
            .filter(m -> Modifier.isPublic(m.getModifiers()) && !Modifier.isStatic(m.getModifiers()))
            .map(m -> m.getName() + Arrays.stream(m.getParameterTypes())
                .map(Class::getSimpleName)
                .collect(Collectors.joining(",", "(", ")")))
            .collect(Collectors.toCollection(TreeSet::new));
    }
}
