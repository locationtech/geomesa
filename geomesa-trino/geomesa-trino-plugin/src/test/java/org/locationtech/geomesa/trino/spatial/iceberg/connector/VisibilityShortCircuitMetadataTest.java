/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.trino.spatial.iceberg.connector;

import io.airlift.slice.Slices;
import io.trino.plugin.iceberg.ColumnIdentity;
import io.trino.plugin.iceberg.IcebergColumnHandle;
import io.trino.plugin.iceberg.IcebergTableHandle;
import io.trino.plugin.iceberg.TableType;
import io.trino.spi.connector.*;
import io.trino.spi.expression.*;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.type.BooleanType;
import io.trino.spi.type.VarcharType;
import org.junit.jupiter.api.Test;
import org.locationtech.geomesa.trino.spatial.iceberg.GeoMesaColumnCatalog;

import java.lang.reflect.Proxy;
import java.util.*;

import static org.assertj.core.api.Assertions.assertThat;
import static io.trino.spi.expression.StandardFunctions.*;

class VisibilityShortCircuitMetadataTest {
    private static final IcebergColumnHandle VIS = new IcebergColumnHandle(ColumnIdentity.primitiveColumnIdentity(1, "__vis__"),
        VarcharType.VARCHAR, List.of(), VarcharType.VARCHAR, false, Optional.empty());
    private static final IcebergColumnHandle OTHER = new IcebergColumnHandle(ColumnIdentity.primitiveColumnIdentity(2, "other"),
        VarcharType.VARCHAR, List.of(), VarcharType.VARCHAR, false, Optional.empty());
    private static final Map<String, ColumnHandle> ASSIGNMENTS = Map.of("v", VIS, "o", OTHER);
    private static final IcebergTableHandle TABLE = new IcebergTableHandle("s", "t", TableType.DATA,
        OptionalLong.empty(), "{}", OptionalInt.empty(), Map.of(), 2, TupleDomain.all(), TupleDomain.all(),
        OptionalLong.empty(), Set.of(), Optional.empty(), "s3://test/t", Map.of(), Optional.empty(), false,
        Optional.empty(), Set.of(), Optional.empty());
    private static Call visible(String variable, String auths) {
        return new Call(BooleanType.BOOLEAN, new FunctionName("is_visible"), List.of(
            new Variable(variable, VarcharType.VARCHAR), new Constant(Slices.utf8Slice(auths), VarcharType.VARCHAR)));
    }
    private static ConnectorSession session() {
        return (ConnectorSession) Proxy.newProxyInstance(ConnectorSession.class.getClassLoader(),
            new Class<?>[]{ConnectorSession.class}, (p, method, args) -> {
                throw new UnsupportedOperationException(method.getName());
            });
    }
    private static final class Delegate implements ConnectorMetadata {
        @Override public SchemaTableName getTableName(ConnectorSession s, ConnectorTableHandle h) { return new SchemaTableName("s", "t"); }
        @Override public Map<String, ColumnHandle> getColumnHandles(ConnectorSession s, ConnectorTableHandle h) { return Map.of("__vis__", VIS); }
        @Override public Optional<ConstraintApplicationResult<ConnectorTableHandle>> applyFilter(ConnectorSession s,
                ConnectorTableHandle h, Constraint c) { return Optional.empty(); }
        @Override public Optional<ProjectionApplicationResult<ConnectorTableHandle>> applyProjection(ConnectorSession s,
                ConnectorTableHandle h, List<ConnectorExpression> p, Map<String, ColumnHandle> a) {
            assertThat(h).isSameAs(TABLE);
            return Optional.of(new ProjectionApplicationResult<>(TABLE, p, List.of(), false));
        }
    }
    private static SpatialConnectorMetadata metadata() {
        GeoMesaColumnCatalog catalog = new GeoMesaColumnCatalog();
        catalog.recordVisibilityColumn(new SchemaTableName("s", "t"), Set.of("__vis__"));
        return new SpatialConnectorMetadata(new Delegate(), catalog, false, false, null, false, Set.of());
    }
    @Test void absorbsExplicitPredicateWithoutResolverOrDeclaredUniverse() {
        var result = metadata().applyFilter(session(), TABLE, new Constraint(TupleDomain.all(), visible("v", "A"), ASSIGNMENTS)).orElseThrow();
        assertThat(result.getRemainingExpression()).contains(Constant.TRUE);
        assertThat(result.getHandle()).isInstanceOf(VisibilityTableHandle.class);
        assertThat(((VisibilityTableHandle) result.getHandle()).auths()).containsExactly("A");
    }
    @Test void absorbsPredicateWithoutSessionProperties() {
        var result = metadata().applyFilter(null, TABLE,
            new Constraint(TupleDomain.all(), visible("v", "A"), ASSIGNMENTS)).orElseThrow();
        assertThat(result.getHandle()).isInstanceOf(VisibilityTableHandle.class);
        assertThat(result.getRemainingExpression()).contains(Constant.TRUE);
    }
    @Test void reentryPreservesAndAddsNarrowerContext() {
        var metadata = metadata();
        var first = metadata.applyFilter(session(), TABLE, new Constraint(TupleDomain.all(), visible("v", "A,B"), ASSIGNMENTS)).orElseThrow().getHandle();
        assertThat(metadata.applyFilter(session(), first, new Constraint(TupleDomain.all(), Constant.TRUE, ASSIGNMENTS))).isEmpty();
        var second = metadata.applyFilter(session(), first, new Constraint(TupleDomain.all(), visible("v", "A"), ASSIGNMENTS)).orElseThrow();
        assertThat(((VisibilityTableHandle) second.getHandle()).auths()).containsExactly("A", "A,B");
    }
    @Test void projectionPreservesEnforcementAndLimitAggregationTopNDecline() {
        var metadata = metadata();
        var handle = new VisibilityTableHandle(TABLE, VIS, List.of("A"));
        assertThat(metadata.applyProjection(session(), handle, List.of(), Map.of()).orElseThrow().getHandle()).isEqualTo(handle);
        assertThat(metadata.applyLimit(null, handle, 1)).isEmpty();
        assertThat(metadata.applyAggregation(null, handle, List.of(), Map.of(), List.of())).isEmpty();
        assertThat(metadata.applyTopN(null, handle, 1, List.of(), Map.of())).isEmpty();
    }
    @Test void visibilityAndSpatialWrappersSurviveProjectionTogether() {
        var spatial = new SpatialTableHandle(TABLE, null, List.of(), 0, 10, 0, 20);
        var visibility = new VisibilityTableHandle(spatial, VIS, List.of("A"));
        var projected = metadata().applyProjection(session(), visibility, List.of(), Map.of()).orElseThrow().getHandle();
        assertThat(projected).isEqualTo(visibility);
        assertThat(((VisibilityTableHandle) projected).delegate()).isEqualTo(spatial);
        assertThat(SpatialTableHandle.unwrap(projected)).isSameAs(TABLE);
    }

    @Test void onlyMandatoryCallsOnVisibilityColumnAreAbsorbed() {
        for (ConnectorExpression expression : List.of(
                visible("o", "A"),
                new Call(BooleanType.BOOLEAN, OR_FUNCTION_NAME, List.of(visible("v", "A"), Constant.TRUE)),
                new Call(BooleanType.BOOLEAN, new FunctionName("$not"), List.of(visible("v", "A"))),
                new Call(BooleanType.BOOLEAN, new FunctionName(Optional.of(new CatalogSchemaName("custom", "schema")), "is_visible"), visible("v", "A").getArguments()),
                new Call(BooleanType.BOOLEAN, new FunctionName("is_visible"), List.of(new Variable("v", VarcharType.VARCHAR), new Variable("o", VarcharType.VARCHAR))))) {
            List<String> auths = new ArrayList<>();
            assertThat(SpatialConnectorMetadata.removeVisibilityConjuncts(expression, VIS, ASSIGNMENTS, auths)).isSameAs(expression);
            assertThat(auths).isEmpty();
        }
    }
    @Test void keepsOtherConjunctsAndLiteralWhitespace() {
        var other = new Call(BooleanType.BOOLEAN, EQUAL_OPERATOR_FUNCTION_NAME, List.of(new Variable("o", VarcharType.VARCHAR), new Constant(Slices.utf8Slice("x"), VarcharType.VARCHAR)));
        var expression = new Call(BooleanType.BOOLEAN, AND_FUNCTION_NAME, List.of(visible("v", " A,B "), other, visible("v", "B")));
        List<String> auths = new ArrayList<>();
        assertThat(SpatialConnectorMetadata.removeVisibilityConjuncts(expression, VIS, ASSIGNMENTS, auths)).isSameAs(other);
        assertThat(auths).containsExactly(" A,B ", "B");
    }
}
