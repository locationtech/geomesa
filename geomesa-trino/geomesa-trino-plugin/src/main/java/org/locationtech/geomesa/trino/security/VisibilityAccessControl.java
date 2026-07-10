/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.trino.security;
import org.locationtech.geomesa.trino.spatial.iceberg.GeoMesaColumnCatalog;

import io.trino.plugin.base.security.AllowAllAccessControl;
import io.trino.spi.connector.ConnectorAccessControl;
import io.trino.spi.connector.ConnectorSecurityContext;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.security.ViewExpression;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.TreeSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Connector-level access control that injects a per-row visibility filter on
 * tables carrying a visibility column, enforcing entitlements for ALL Trino
 * consumers of the {@code spatial_iceberg} catalog (direct SQL / JDBC / BI),
 * complementing the datastore-layer enforcement used by GeoTools clients.
 *
 * <p><strong>Allow-all baseline.</strong> Every {@link ConnectorAccessControl}
 * method DENIES by default, so a connector that installs one is expected to own
 * authorization for the catalog. This feature is purely additive — it must not
 * restrict anything beyond row visibility — so it extends
 * {@link AllowAllAccessControl}, reproducing the permissive baseline of
 * Iceberg's (absent) connector access control. Only {@link #getRowFilters}
 * adds behavior.
 *
 * <p>The visibility column is detected at analysis time by
 * {@code SpatialConnectorMetadata.getColumnHandles} and read here from the shared
 * {@link GeoMesaColumnCatalog}. Metadata tables (information_schema, Iceberg
 * {@code $}-metadata) are skipped. For any other (data) table that has not been
 * observed, the filter fails closed (hides all rows) rather than risk leaking a
 * vis-bearing table. Boundary: this protects only {@code spatial_iceberg}; the
 * plain {@code iceberg} catalog is not wrapped and must not be exposed to
 * untrusted users.
 */
public final class VisibilityAccessControl extends AllowAllAccessControl {

    private static final Logger LOG = LoggerFactory.getLogger(VisibilityAccessControl.class);

    private final String catalog;
    private final GeoMesaColumnCatalog geomCatalog;
    private final AuthorizationResolver resolver;

    /**
     * Builds the access control for a single catalog.
     *
     * @param catalog name of the catalog this access control guards
     * @param geomCatalog shared column catalog used to detect visibility columns
     * @param resolver resolves a session identity to its authorization tokens
     */
    public VisibilityAccessControl(String catalog, GeoMesaColumnCatalog geomCatalog,
                                   AuthorizationResolver resolver) {
        this.catalog = catalog;
        this.geomCatalog = geomCatalog;
        this.resolver = resolver;
    }

    /**
     * The one method that adds behavior: a row filter for tables with a
     * visibility column, evaluated by the global {@code is_visible} UDF.
     *
     * @param context the connector security context (carries the session identity)
     * @param table the table being accessed
     * @return row filters to apply; empty list if the table needs none
     */
    @Override
    public List<ViewExpression> getRowFilters(ConnectorSecurityContext context, SchemaTableName table) {
        // Metadata/system tables (information_schema, Iceberg "$"-metadata) are
        // never visibility-controlled and reach here unobserved; skip them
        // explicitly so the fail-closed branch below doesn't empty SHOW TABLES /
        // information_schema.
        if (isMetadataTable(table)) {
            return List.of();
        }
        Optional<GeoMesaColumnCatalog.ObservedVisibility> observed =
            geomCatalog.visibilityColumn(table);
        if (observed.isEmpty()) {
            // A real data table reached row-filter analysis without first being
            // observed via getColumnHandles (e.g. an unexpected planner path or
            // cold worker). Fail closed — hide all rows rather than risk leaking
            // a vis-bearing table. Normal scans observe columns first, so this
            // should not fire in practice.
            LOG.warn("Visibility column not observed for " + catalog + "." + table
                + " before row-filter analysis; hiding all rows (fail-closed)");
            return List.of(viewExpression(context, table, "false"));
        }
        Optional<String> visColumn = observed.get().column();
        if (visColumn.isEmpty()) {
            return List.of();  // observed: table has no visibility column
        }
        // Sort for a deterministic, cache-friendly auth literal (order is
        // irrelevant to the is_visible UDF).
        List<String> auths =
            new ArrayList<>(new TreeSet<>(resolver.authorizationsFor(context.getIdentity())));
        return List.of(viewExpression(context, table,
            VisibilityRowFilter.conjunct(visColumn.get(), auths)));
    }

    /** information_schema and Iceberg {@code $}-suffixed metadata tables carry no
     *  visibility column and must never be row-filtered. */
    private static boolean isMetadataTable(SchemaTableName table) {
        return "information_schema".equals(table.getSchemaName())
            || table.getTableName().contains("$");
    }

    private ViewExpression viewExpression(ConnectorSecurityContext context,
                                          SchemaTableName table, String expression) {
        return ViewExpression.builder()
            .identity(context.getIdentity().getUser())
            .catalog(catalog)
            .schema(table.getSchemaName())
            .expression(expression)
            .build();
    }
}
