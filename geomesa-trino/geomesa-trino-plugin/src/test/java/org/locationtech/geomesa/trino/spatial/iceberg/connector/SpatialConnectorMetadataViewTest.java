/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.trino.spatial.iceberg.connector;

import io.trino.spi.QueryId;
import io.trino.spi.connector.*;
import io.trino.spi.security.ConnectorIdentity;
import io.trino.spi.security.ViewExpression;
import io.trino.spi.type.TypeId;
import org.junit.jupiter.api.Test;
import org.locationtech.geomesa.trino.security.AuthorizationResolver;
import org.locationtech.geomesa.trino.security.VisibilityAccessControl;
import org.locationtech.geomesa.trino.spatial.iceberg.GeoMesaColumnCatalog;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * A view is resolved through {@code getView}, never {@code getColumnHandles}, so without
 * the observation done there the access control sees an unobserved name and hides every
 * row. These cover that path and the DEFINER→INVOKER rewrite that makes leaving a view
 * unfiltered sound.
 */
class SpatialConnectorMetadataViewTest {

    private static final String CATALOG = "spatial_iceberg";
    private static final SchemaTableName VIEW = new SchemaTableName("spatial", "composite");

    private static final AuthorizationResolver RESOLVER = id -> "alice".equals(id.getUser()) ? Set.of("basic", "privileged") : Set.of();

    private static ConnectorSecurityContext ctx(String user) {
        return new ConnectorSecurityContext(
            new ConnectorTransactionHandle() {},
            ConnectorIdentity.forUser(user).withGroups(Set.of()).build(),
            new QueryId("q"));
    }

    /** Delegate returning one view definition, as the iceberg connector would. */
    private static class FakeMetadata implements ConnectorMetadata {
        private final ConnectorViewDefinition definition;
        FakeMetadata(ConnectorViewDefinition definition) { this.definition = definition; }

        @Override
        public Optional<ConnectorViewDefinition> getView(ConnectorSession session, SchemaTableName viewName) {
            return VIEW.equals(viewName) ? Optional.of(definition) : Optional.empty();
        }

        @Override
        public Map<SchemaTableName, ConnectorViewDefinition> getViews(ConnectorSession session, Optional<String> schemaName) {
            return Map.of(VIEW, definition);
        }
    }

    private static ConnectorViewDefinition definition(boolean runAsInvoker, String... columns) {
        List<ConnectorViewDefinition.ViewColumn> cols = java.util.Arrays.stream(columns)
            .map(c -> new ConnectorViewDefinition.ViewColumn(c, TypeId.of("varchar"), Optional.empty()))
            .toList();
        return new ConnectorViewDefinition(
            "SELECT 1",
            Optional.of(CATALOG),
            Optional.of("spatial"),
            cols,
            Optional.empty(),
            runAsInvoker ? Optional.empty() : Optional.of("viewowner"),
            runAsInvoker,
            List.of());
    }

    private static SpatialConnectorMetadata metadata(GeoMesaColumnCatalog cat, ConnectorViewDefinition def, boolean useInvokerAuths) {
        return new SpatialConnectorMetadata(new FakeMetadata(def), cat, false, useInvokerAuths);
    }

    private static List<ViewExpression> filtersFor(GeoMesaColumnCatalog cat) {
        return new VisibilityAccessControl(CATALOG, cat, RESOLVER).getRowFilters(ctx("alice"), VIEW);
    }

    @Test
    void unresolvedViewFailsClosed() {
        GeoMesaColumnCatalog cat = new GeoMesaColumnCatalog();
        List<ViewExpression> filters = filtersFor(cat);
        assertThat(filters).hasSize(1);
        assertThat(filters.get(0).getExpression()).isEqualTo("false");
    }

    @Test
    void resolvingViewWithVisColumnEmitsRealFilter() {
        GeoMesaColumnCatalog cat = new GeoMesaColumnCatalog();
        metadata(cat, definition(true, "__fid__", "__vis__", "taxiid"), true).getView(null, VIEW);
        assertThat(filtersFor(cat)).hasSize(1);
        assertThat(filtersFor(cat).get(0).getExpression())
            .isEqualTo("is_visible(\"__vis__\", 'basic,privileged')");
    }

    @Test
    void resolvingViewWithoutVisColumnPassesThrough() {
        // No __vis__ on the view: the base tables enforce their own visibility (as the
        // invoker), so the view itself must not be filtered — and must not fail closed.
        GeoMesaColumnCatalog cat = new GeoMesaColumnCatalog();
        metadata(cat, definition(true, "taxiid", "dtg"), true).getView(null, VIEW);
        assertThat(filtersFor(cat)).isEmpty();
    }

    @Test
    void definerViewIsRewrittenToInvoker() {
        GeoMesaColumnCatalog cat = new GeoMesaColumnCatalog();
        ConnectorViewDefinition definer = definition(false, "__vis__");
        assertThat(definer.isRunAsInvoker()).isFalse();
        assertThat(definer.getOwner()).contains("viewowner");

        ConnectorViewDefinition out =
            metadata(cat, definer, true).getView(null, VIEW).orElseThrow();
        assertThat(out.isRunAsInvoker()).isTrue();
        // ConnectorViewDefinition rejects an owner alongside runAsInvoker.
        assertThat(out.getOwner()).isEmpty();
        // Everything else is preserved.
        assertThat(out.getOriginalSql()).isEqualTo(definer.getOriginalSql());
        assertThat(out.getCatalog()).isEqualTo(definer.getCatalog());
        assertThat(out.getSchema()).isEqualTo(definer.getSchema());
        assertThat(out.getColumns()).hasSameSizeAs(definer.getColumns());
    }

    @Test
    void invokerViewIsReturnedUnchanged() {
        GeoMesaColumnCatalog cat = new GeoMesaColumnCatalog();
        ConnectorViewDefinition invoker = definition(true, "__vis__");
        assertThat(metadata(cat, invoker, true).getView(null, VIEW)).contains(invoker);
    }

    @Test
    void definerViewPreservedWhenRewriteDisabled() {
        GeoMesaColumnCatalog cat = new GeoMesaColumnCatalog();
        ConnectorViewDefinition definer = definition(false, "__vis__");
        ConnectorViewDefinition out =
            metadata(cat, definer, false).getView(null, VIEW).orElseThrow();
        assertThat(out.isRunAsInvoker()).isFalse();
        assertThat(out.getOwner()).contains("viewowner");
        // Observation still happens, so the view is not hidden.
        assertThat(filtersFor(cat)).hasSize(1);
        assertThat(filtersFor(cat).get(0).getExpression())
            .isEqualTo("is_visible(\"__vis__\", 'basic,privileged')");
    }

    @Test
    void getViewsObservesAndRewritesToo() {
        GeoMesaColumnCatalog cat = new GeoMesaColumnCatalog();
        Map<SchemaTableName, ConnectorViewDefinition> views =
            metadata(cat, definition(false, "__vis__"), true).getViews(null, Optional.of("spatial"));
        assertThat(views.get(VIEW).isRunAsInvoker()).isTrue();
        assertThat(filtersFor(cat)).hasSize(1);
        assertThat(filtersFor(cat).get(0).getExpression())
            .isEqualTo("is_visible(\"__vis__\", 'basic,privileged')");
    }

    @Test
    void missingViewIsNotObserved() {
        // getView returning empty must not record anything, or a dropped view would
        // silently become unfiltered.
        GeoMesaColumnCatalog cat = new GeoMesaColumnCatalog();
        SchemaTableName other = new SchemaTableName("spatial", "absent");
        assertThat(metadata(cat, definition(true, "__vis__"), true).getView(null, other)).isEmpty();
        assertThat(cat.visibilityColumn(other)).isEmpty();
    }
}
