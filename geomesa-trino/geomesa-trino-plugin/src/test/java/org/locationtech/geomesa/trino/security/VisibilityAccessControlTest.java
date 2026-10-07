/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.trino.security;
import org.locationtech.geomesa.trino.spatial.iceberg.GeoMesaColumnCatalog;

import io.trino.spi.QueryId;
import io.trino.spi.connector.ConnectorSecurityContext;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.security.ConnectorIdentity;
import io.trino.spi.security.ViewExpression;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

class VisibilityAccessControlTest {

    private static final String CATALOG = "spatial_iceberg";
    private static final SchemaTableName TABLE = new SchemaTableName("spatial", "observations");

    // alice -> {basic, privileged}; everyone else -> empty (fail-closed).
    private static final AuthorizationResolver RESOLVER = id ->
        "alice".equals(id.getUser()) ? Set.of("basic", "privileged") : Set.of();

    private static ConnectorSecurityContext ctx(String user) {
        return new ConnectorSecurityContext(
            new ConnectorTransactionHandle() {},
            ConnectorIdentity.forUser(user).withGroups(Set.of()).build(),
            new QueryId("q"));
    }

    private VisibilityAccessControl control(GeoMesaColumnCatalog cat) {
        return new VisibilityAccessControl(CATALOG, cat, RESOLVER);
    }

    @Test
    void emitsFilterForVisColumn() {
        GeoMesaColumnCatalog cat = new GeoMesaColumnCatalog();
        cat.recordVisibilityColumn(TABLE, Set.of("__fid__", "geom", "__vis__"));
        List<ViewExpression> filters = control(cat).getRowFilters(ctx("alice"), TABLE);
        assertThat(filters).hasSize(1);
        ViewExpression f = filters.get(0);
        // auths sorted for determinism: {basic, privileged} -> "basic,privileged"
        assertThat(f.getExpression()).isEqualTo("is_visible(\"__vis__\", 'basic,privileged')");
        assertThat(f.getCatalog()).contains(CATALOG);
        assertThat(f.getSchema()).contains("spatial");
    }

    @Test
    void bareVisibilitiesColumnDoesNotEngageEnforcement() {
        // A user attribute merely named "visibilities" is ordinary data — only the
        // companion-style __vis__ (which the FSDS populates) engages filtering.
        GeoMesaColumnCatalog cat = new GeoMesaColumnCatalog();
        cat.recordVisibilityColumn(TABLE, Set.of("geom", "visibilities"));
        assertThat(control(cat).getRowFilters(ctx("alice"), TABLE)).isEmpty();
    }

    @Test
    void usesCompanionVisColumn() {
        GeoMesaColumnCatalog cat = new GeoMesaColumnCatalog();
        cat.recordVisibilityColumn(TABLE, Set.of("geom", "__vis__"));
        assertThat(control(cat).getRowFilters(ctx("alice"), TABLE).get(0).getExpression())
            .isEqualTo("is_visible(\"__vis__\", 'basic,privileged')");
    }

    @Test
    void noFilterWhenTableHasNoVisibilityColumn() {
        GeoMesaColumnCatalog cat = new GeoMesaColumnCatalog();
        cat.recordVisibilityColumn(TABLE, Set.of("__fid__", "geom", "dtg"));
        assertThat(control(cat).getRowFilters(ctx("alice"), TABLE)).isEmpty();
    }

    @Test
    void emptyAuthsForUnknownUserEmitEmptyLiteral() {
        GeoMesaColumnCatalog cat = new GeoMesaColumnCatalog();
        cat.recordVisibilityColumn(TABLE, Set.of("geom", "__vis__"));
        assertThat(control(cat).getRowFilters(ctx("nobody"), TABLE).get(0).getExpression())
            .isEqualTo("is_visible(\"__vis__\", '')");
    }

    @Test
    void notObservedDataTableFailsClosed() {
        // A real (user-schema) data table not observed via getColumnHandles →
        // hide all rows rather than risk leaking a vis-bearing table.
        GeoMesaColumnCatalog cat = new GeoMesaColumnCatalog();
        List<ViewExpression> filters = control(cat).getRowFilters(ctx("alice"), TABLE);
        assertThat(filters).hasSize(1);
        assertThat(filters.get(0).getExpression()).isEqualTo("false");
    }

    @Test
    void informationSchemaTableEmitsNoFilter() {
        // Metadata table reached unobserved → must NOT be filtered (else SHOW
        // TABLES / information_schema would be emptied).
        GeoMesaColumnCatalog cat = new GeoMesaColumnCatalog();
        SchemaTableName infoTable = new SchemaTableName("information_schema", "tables");
        assertThat(control(cat).getRowFilters(ctx("alice"), infoTable)).isEmpty();
    }

    @Test
    void icebergMetadataTableEmitsNoFilter() {
        // Iceberg "$"-suffixed metadata table → not filtered.
        GeoMesaColumnCatalog cat = new GeoMesaColumnCatalog();
        SchemaTableName metaTable = new SchemaTableName("spatial", "observations$snapshots");
        assertThat(control(cat).getRowFilters(ctx("alice"), metaTable)).isEmpty();
    }
    @Test
    void preservesDelegateRestrictionsFiltersAndMasks() {
        GeoMesaColumnCatalog catalog = new GeoMesaColumnCatalog();
        catalog.recordVisibilityColumn(TABLE, Set.of("__vis__"));
        ViewExpression existing = ViewExpression.builder().expression("tenant = 'alice'").build();
        ViewExpression mask = ViewExpression.builder().expression("NULL").build();
        var policy = new io.trino.plugin.base.security.ReadOnlyAccessControl() {
            @Override
            public List<ViewExpression> getRowFilters(ConnectorSecurityContext context, SchemaTableName table) {
                return List.of(existing);
            }
            @Override
            public java.util.Map<io.trino.spi.connector.ColumnSchema, ViewExpression> getColumnMasks(
                    ConnectorSecurityContext context, SchemaTableName table,
                    List<io.trino.spi.connector.ColumnSchema> columns) {
                return java.util.Map.of(columns.get(0), mask);
            }
        };
        var control = new VisibilityAccessControl(CATALOG, catalog, RESOLVER, policy);
        org.assertj.core.api.Assertions.assertThatThrownBy(() -> control.checkCanDropTable(ctx("alice"), TABLE))
            .isInstanceOf(io.trino.spi.security.AccessDeniedException.class);
        assertThat(control.getRowFilters(ctx("alice"), TABLE)).hasSize(2).startsWith(existing);
        var column = io.trino.spi.connector.ColumnSchema.builder().setName("secret")
            .setType(io.trino.spi.type.VarcharType.VARCHAR).build();
        assertThat(control.getColumnMasks(ctx("alice"), TABLE, List.of(column))).containsEntry(column, mask);
        SchemaTableName metadata = new SchemaTableName("spatial", "observations$files");
        assertThat(control.getRowFilters(ctx("alice"), metadata)).containsExactly(existing);
    }

    @Test
    void connectorComposesItsConfiguredPolicyAndSupportsSystemSecurity() {
        var delegate = new io.trino.spi.connector.Connector() {
            @Override public void shutdown() {}
            @Override
            public io.trino.spi.connector.ConnectorAccessControl getAccessControl() {
                return new io.trino.plugin.base.security.ReadOnlyAccessControl();
            }
        };
        var connector = new org.locationtech.geomesa.trino.spatial.iceberg.connector.SpatialConnector(
            delegate, CATALOG, RESOLVER);
        org.assertj.core.api.Assertions.assertThatThrownBy(
            () -> connector.getAccessControl().checkCanDropTable(ctx("alice"), TABLE))
            .isInstanceOf(io.trino.spi.security.AccessDeniedException.class);

        // SYSTEM security supplies no connector policy; the default getAccessControl throws.
        var system = new org.locationtech.geomesa.trino.spatial.iceberg.connector.SpatialConnector(
            new io.trino.spi.connector.Connector() {
                @Override public void shutdown() {}
            }, CATALOG, RESOLVER);
        system.getAccessControl().checkCanDropTable(ctx("alice"), TABLE);
        assertThat(system.getAccessControl().getRowFilters(ctx("alice"), TABLE))
            .extracting(ViewExpression::getExpression).containsExactly("false");
    }

}
