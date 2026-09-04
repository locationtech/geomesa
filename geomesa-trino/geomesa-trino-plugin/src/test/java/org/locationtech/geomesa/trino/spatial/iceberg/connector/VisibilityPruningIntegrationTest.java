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
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ConnectorMetadata;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTableVersion;
import io.trino.spi.connector.Constraint;
import io.trino.spi.connector.ConstraintApplicationResult;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.expression.Constant;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.security.ConnectorIdentity;
import io.trino.spi.type.TimeZoneKey;
import io.trino.spi.type.VarcharType;
import org.junit.jupiter.api.Test;
import org.locationtech.geomesa.trino.security.AuthorizationResolver;
import org.locationtech.geomesa.trino.spatial.iceberg.GeoMesaColumnCatalog;

import java.time.Instant;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * End-to-end tests of visibility-column domain pushdown through
 * {@link SpatialConnectorMetadata#applyFilter}, exercising the full wiring
 * (resolver → identity → domain → delegate constraint) that
 * {@link org.locationtech.geomesa.trino.security.VisibilityDomainPruningTest}
 * cannot reach since it tests the pure domain-construction helper in isolation.
 */
class VisibilityPruningIntegrationTest {

    private static final SchemaTableName TABLE = new SchemaTableName("s", "t");
    private static final AuthorizationResolver EMPTY_RESOLVER = id -> Set.of();
    private static final AuthorizationResolver USER_RESOLVER = id -> Set.of("user");

    private static ConnectorSession sessionFor(String user) {
        ConnectorIdentity identity = ConnectorIdentity.forUser(user).withGroups(Set.of()).build();
        return new ConnectorSession() {
            @Override public String getQueryId() { return "q"; }
            @Override public Optional<String> getSource() { return Optional.empty(); }
            @Override public ConnectorIdentity getIdentity() { return identity; }
            @Override public TimeZoneKey getTimeZoneKey() { return TimeZoneKey.UTC_KEY; }
            @Override public Locale getLocale() { return Locale.ENGLISH; }
            @Override public Optional<String> getTraceToken() { return Optional.empty(); }
            @Override public Instant getStart() { return Instant.EPOCH; }
            @Override public <T> T getProperty(String name, Class<T> type) {
                throw new UnsupportedOperationException();
            }
        };
    }

    private static ConnectorTableHandle fakeHandle() {
        return new ConnectorTableHandle() {};
    }

    private static Constraint allConstraint() {
        return new Constraint(TupleDomain.all(), Constant.TRUE, Map.of());
    }

    /** Delegate exposing a single VARCHAR {@code __vis__} column and capturing
     *  the {@link Constraint} passed to {@code applyFilter} for assertions. */
    private static class VisDelegate implements ConnectorMetadata {
        final IcebergColumnHandle visHandle;
        Constraint lastConstraint;

        VisDelegate() {
            ColumnIdentity id = ColumnIdentity.primitiveColumnIdentity(1, "__vis__");
            this.visHandle = new IcebergColumnHandle(
                id, VarcharType.VARCHAR, List.of(1), VarcharType.VARCHAR, true, Optional.empty());
        }

        @Override
        public SchemaTableName getTableName(ConnectorSession session, ConnectorTableHandle table) {
            return TABLE;
        }

        @Override
        public Map<String, ColumnHandle> getColumnHandles(ConnectorSession session,
                                                           ConnectorTableHandle tableHandle) {
            return Map.of("__vis__", visHandle);
        }

        @Override
        public Optional<ConstraintApplicationResult<ConnectorTableHandle>> applyFilter(
                ConnectorSession session, ConnectorTableHandle handle, Constraint constraint) {
            this.lastConstraint = constraint;
            return Optional.empty();
        }

        @Override
        public List<String> listSchemaNames(ConnectorSession session) {
            return List.of();
        }

        @Override
        public ConnectorTableHandle getTableHandle(ConnectorSession session, SchemaTableName tableName,
                Optional<ConnectorTableVersion> startVersion, Optional<ConnectorTableVersion> endVersion) {
            return null;
        }

        @Override
        public ColumnMetadata getColumnMetadata(ConnectorSession session, ConnectorTableHandle tableHandle,
                                                 ColumnHandle columnHandle) {
            return null;
        }
    }

    private static boolean hasVisDomain(VisDelegate delegate) {
        if (delegate.lastConstraint == null) {
            return false;
        }
        return delegate.lastConstraint.getSummary().getDomains()
            .map(d -> d.containsKey(delegate.visHandle))
            .orElse(false);
    }

    @Test
    void emptyAuthsInjectsNullOnlyDomainOnVisColumn() {
        VisDelegate delegate = new VisDelegate();
        SpatialConnectorMetadata meta = new SpatialConnectorMetadata(
            delegate, new GeoMesaColumnCatalog(), false, EMPTY_RESOLVER, true, Set.of());
        ConnectorSession session = sessionFor("nobody");
        ConnectorTableHandle handle = fakeHandle();

        // Analysis phase records the vis column, mirroring the production query path.
        meta.getColumnHandles(session, handle);
        meta.applyFilter(session, handle, allConstraint());

        assertThat(hasVisDomain(delegate)).isTrue();
        Domain injected = delegate.lastConstraint.getSummary().getDomains().orElseThrow().get(delegate.visHandle);
        assertThat(injected.isOnlyNull()).isTrue();
    }

    @Test
    void nonEmptyAuthsWithoutDeclaredExpressionsInjectsNoDomain() {
        VisDelegate delegate = new VisDelegate();
        SpatialConnectorMetadata meta = new SpatialConnectorMetadata(
            delegate, new GeoMesaColumnCatalog(), false, USER_RESOLVER, true, Set.of());
        ConnectorSession session = sessionFor("alice");
        ConnectorTableHandle handle = fakeHandle();

        meta.getColumnHandles(session, handle);
        meta.applyFilter(session, handle, allConstraint());

        // expressionDomain needs a declared candidate universe; with none, a non-empty
        // auth set gets no domain (only the empty-auths tier is unconditional).
        assertThat(hasVisDomain(delegate)).isFalse();
    }

    @Test
    void nonEmptyAuthsWithDeclaredExpressionsInjectsExpressionDomain() {
        VisDelegate delegate = new VisDelegate();
        SpatialConnectorMetadata meta = new SpatialConnectorMetadata(
            delegate, new GeoMesaColumnCatalog(), false, USER_RESOLVER, true, Set.of("user", "admin"));
        ConnectorSession session = sessionFor("alice");
        ConnectorTableHandle handle = fakeHandle();

        meta.getColumnHandles(session, handle);
        meta.applyFilter(session, handle, allConstraint());

        // USER_RESOLVER grants {"user"}: expressionDomain admits the declared "user" value
        // (and NULL) but not "admin", which the caller's auths cannot satisfy.
        assertThat(hasVisDomain(delegate)).isTrue();
        Domain injected = delegate.lastConstraint.getSummary().getDomains().orElseThrow().get(delegate.visHandle);
        assertThat(injected.includesNullableValue(Slices.utf8Slice("user"))).isTrue();
        assertThat(injected.includesNullableValue(null)).isTrue();
        assertThat(injected.includesNullableValue(Slices.utf8Slice("admin"))).isFalse();
    }

    @Test
    void pruningDisabledInjectsNoDomainEvenForEmptyAuths() {
        // Master gate off: the always-on empty-auths tier (which would otherwise inject
        // vis IS NULL for a caller with no auths) must not fire. Behavior reverts to
        // pre-feature — only the is_visible() row filter runs, no domain is pushed down.
        VisDelegate delegate = new VisDelegate();
        SpatialConnectorMetadata meta = new SpatialConnectorMetadata(
            delegate, new GeoMesaColumnCatalog(), false, EMPTY_RESOLVER, false, Set.of());
        ConnectorSession session = sessionFor("nobody");
        ConnectorTableHandle handle = fakeHandle();

        meta.getColumnHandles(session, handle);
        meta.applyFilter(session, handle, allConstraint());

        assertThat(hasVisDomain(delegate)).isFalse();
    }

    @Test
    void pruningDisabledInjectsNoDomainEvenWithDeclaredExpressions() {
        // Master gate off overrides a declared expression universe: the expression tier
        // is skipped despite USER_RESOLVER + candidates that would otherwise prune.
        VisDelegate delegate = new VisDelegate();
        SpatialConnectorMetadata meta = new SpatialConnectorMetadata(
            delegate, new GeoMesaColumnCatalog(), false, USER_RESOLVER, false, Set.of("user", "admin"));
        ConnectorSession session = sessionFor("alice");
        ConnectorTableHandle handle = fakeHandle();

        meta.getColumnHandles(session, handle);
        meta.applyFilter(session, handle, allConstraint());

        assertThat(hasVisDomain(delegate)).isFalse();
    }

    @Test
    void noResolverConfiguredNeverTouchesSessionOrInjectsDomain() {
        // The 3-arg constructor (used throughout SpatialConnectorMetadataTest) leaves
        // resolver null; applyFilter must short-circuit before dereferencing a null
        // session, exactly like every pre-existing spatial-only test relies on.
        VisDelegate delegate = new VisDelegate();
        SpatialConnectorMetadata meta = new SpatialConnectorMetadata(delegate, new GeoMesaColumnCatalog(), false);
        ConnectorTableHandle handle = fakeHandle();

        meta.getColumnHandles(null, handle);
        meta.applyFilter(null, handle, allConstraint());

        assertThat(hasVisDomain(delegate)).isFalse();
    }

    @Test
    void alreadyRoundTrippedVisDomainIsNotReinjected() {
        VisDelegate delegate = new VisDelegate();
        SpatialConnectorMetadata meta = new SpatialConnectorMetadata(
            delegate, new GeoMesaColumnCatalog(), false, EMPTY_RESOLVER, true, Set.of());
        ConnectorSession session = sessionFor("nobody");
        ConnectorTableHandle handle = fakeHandle();
        meta.getColumnHandles(session, handle);

        // Planner already carries a (different) domain for the vis column.
        Domain existing = Domain.singleValue(VarcharType.VARCHAR, Slices.utf8Slice("public"));
        Constraint constraint = new Constraint(
            TupleDomain.withColumnDomains(Map.of(delegate.visHandle, existing)),
            Constant.TRUE, Map.of());

        meta.applyFilter(session, handle, constraint);

        // Not re-injected/overwritten with the onlyNull() domain — the existing
        // round-tripped domain is passed through unchanged.
        Domain seen = delegate.lastConstraint.getSummary().getDomains().orElseThrow().get(delegate.visHandle);
        assertThat(seen).isEqualTo(existing);
    }
}
