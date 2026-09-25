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
import io.trino.spi.expression.Call;
import io.trino.spi.expression.ConnectorExpression;
import io.trino.spi.expression.Constant;
import io.trino.spi.expression.FunctionName;
import io.trino.spi.expression.Variable;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.security.ConnectorIdentity;
import io.trino.spi.type.BigintType;
import io.trino.spi.type.BooleanType;
import io.trino.spi.type.TimeZoneKey;
import io.trino.spi.type.VarcharType;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.locationtech.geomesa.trino.security.AuthorizationResolver;
import org.locationtech.geomesa.trino.spatial.iceberg.GeoMesaColumnCatalog;

import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static io.trino.spi.expression.StandardFunctions.AND_FUNCTION_NAME;
import static io.trino.spi.expression.StandardFunctions.EQUAL_OPERATOR_FUNCTION_NAME;
import static io.trino.spi.expression.StandardFunctions.OR_FUNCTION_NAME;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Specifies how an explicit {@code is_visible(<vis column>, '<auths>')} query predicate should
 * feed visibility-column file pruning in {@link SpatialConnectorMetadata#applyFilter}.
 *
 * <p>A top-level (AND-ed) {@code is_visible} conjunct on the visibility column with a literal
 * auths argument means no row outside what those auths admit can be returned, so the pruning
 * domain should admit only values visible to <em>both</em> the resolver's auths and the
 * predicate's auths. This lets a service identity with broad mapped auths get pruning for the end
 * user it narrows to.
 *
 * <p>{@link Honored} describes the behavior to build; its tests fail until the predicate is used.
 * {@link Guards} describes cases that must never narrow (or widen) the domain; they pass today and
 * must keep passing once the predicate is honored.
 */
class VisibilityPruningPredicateTest {

    private static final SchemaTableName TABLE = new SchemaTableName("s", "t");
    private static final Set<String> UNIVERSE = Set.of("admin", "ops", "finance", "ops&finance");
    private static final AuthorizationResolver ALL_AUTHS = id -> Set.of("admin", "ops", "finance");
    private static final AuthorizationResolver OPS_ONLY = id -> Set.of("ops");

    /** The engine's variable name for the vis column; deliberately not "__vis__", so the column
     *  must be resolved through {@link Constraint#getAssignments()}. */
    private static final String VIS_VAR = "__vis___7";
    private static final String ID_VAR = "id_3";
    private static final String OTHER_VAR = "other_5";

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

    /** Delegate exposing {@code __vis__}, {@code id} and {@code other} columns and capturing the
     *  {@link Constraint} passed to {@code applyFilter}. */
    private static class VisDelegate implements ConnectorMetadata {
        final IcebergColumnHandle visHandle = varchar(1, "__vis__");
        final IcebergColumnHandle idHandle = new IcebergColumnHandle(
            ColumnIdentity.primitiveColumnIdentity(2, "id"), BigintType.BIGINT, List.of(2),
            BigintType.BIGINT, true, Optional.empty());
        final IcebergColumnHandle otherHandle = varchar(3, "other");
        Constraint lastConstraint;

        private static IcebergColumnHandle varchar(int id, String name) {
            return new IcebergColumnHandle(ColumnIdentity.primitiveColumnIdentity(id, name),
                VarcharType.VARCHAR, List.of(id), VarcharType.VARCHAR, true, Optional.empty());
        }

        Map<String, ColumnHandle> assignments() {
            Map<String, ColumnHandle> m = new HashMap<>();
            m.put(VIS_VAR, visHandle);
            m.put(ID_VAR, idHandle);
            m.put(OTHER_VAR, otherHandle);
            return m;
        }

        @Override
        public SchemaTableName getTableName(ConnectorSession session, ConnectorTableHandle table) {
            return TABLE;
        }

        @Override
        public Map<String, ColumnHandle> getColumnHandles(ConnectorSession session,
                                                           ConnectorTableHandle tableHandle) {
            return Map.of("__vis__", visHandle, "id", idHandle, "other", otherHandle);
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

    // ---- expression builders, in the shapes the engine hands to applyFilter ----

    private static Constant varcharLiteral(String value) {
        return new Constant(Slices.utf8Slice(value), VarcharType.createVarcharType(Math.max(1, value.length())));
    }

    /** {@code is_visible(<var>, '<auths>')}. */
    private static ConnectorExpression isVisible(String var, String auths) {
        return isVisible(new Variable(var, VarcharType.VARCHAR), varcharLiteral(auths));
    }

    private static ConnectorExpression isVisible(ConnectorExpression vis, ConnectorExpression auths) {
        return new Call(BooleanType.BOOLEAN, new FunctionName("is_visible"), List.of(vis, auths));
    }

    private static ConnectorExpression and(ConnectorExpression... terms) {
        return new Call(BooleanType.BOOLEAN, AND_FUNCTION_NAME, List.of(terms));
    }

    private static ConnectorExpression or(ConnectorExpression... terms) {
        return new Call(BooleanType.BOOLEAN, OR_FUNCTION_NAME, List.of(terms));
    }

    /** {@code id = 1}. */
    private static ConnectorExpression idEqualsOne() {
        return new Call(BooleanType.BOOLEAN, EQUAL_OPERATOR_FUNCTION_NAME,
            List.of(new Variable(ID_VAR, BigintType.BIGINT), new Constant(1L, BigintType.BIGINT)));
    }

    // ---- harness ----

    /** Runs applyFilter as {@code user} and returns the vis domain pushed to Iceberg (null if none). */
    private static Domain pushedVisDomain(AuthorizationResolver resolver, boolean pruningEnabled,
                                          ConnectorExpression expression) {
        VisDelegate delegate = new VisDelegate();
        SpatialConnectorMetadata meta = new SpatialConnectorMetadata(
            delegate, new GeoMesaColumnCatalog(), false, resolver, pruningEnabled, UNIVERSE);
        ConnectorSession session = sessionFor("geomesa");
        ConnectorTableHandle handle = new ConnectorTableHandle() {};
        meta.getColumnHandles(session, handle);
        meta.applyFilter(session, handle, new Constraint(TupleDomain.all(), expression, delegate.assignments()));
        assertThat(delegate.lastConstraint).as("delegate applyFilter was called").isNotNull();
        return delegate.lastConstraint.getSummary().getDomains()
            .map(d -> d.get(delegate.visHandle))
            .orElse(null);
    }

    private static Domain pushedVisDomain(AuthorizationResolver resolver, ConnectorExpression expression) {
        return pushedVisDomain(resolver, true, expression);
    }

    private static Domain noPredicate(AuthorizationResolver resolver) {
        return pushedVisDomain(resolver, Constant.TRUE);
    }

    private static boolean admits(Domain domain, String vis) {
        return domain.includesNullableValue(vis == null ? null : Slices.utf8Slice(vis));
    }

    /** The unrestricted values (NULL and '') are always admitted, whatever the auths. */
    private static void assertAdmitsUnrestricted(Domain domain) {
        assertThat(admits(domain, null)).as("admits NULL").isTrue();
        assertThat(admits(domain, "")).as("admits ''").isTrue();
    }

    /**
     * The behavior to build. Each test fails until {@code applyFilter} narrows the pruning domain
     * by top-level {@code is_visible} conjuncts.
     */
    @Nested
    class Honored {

        @Test
        void narrowPredicateNarrowsDomainToIntersection() {
            // Resolver grants {admin, ops, finance}; the query narrows to {ops}.
            Domain domain = pushedVisDomain(ALL_AUTHS, isVisible(VIS_VAR, "ops"));

            assertThat(domain).isNotNull();
            assertThat(admits(domain, "ops")).isTrue();
            assertThat(admits(domain, "admin")).as("admin file should be pruned").isFalse();
            assertThat(admits(domain, "finance")).as("finance file should be pruned").isFalse();
            assertThat(admits(domain, "ops&finance")).as("ops&finance file should be pruned").isFalse();
            assertAdmitsUnrestricted(domain);
        }

        @Test
        void narrowPredicatePrunesExactlyLikeResolvingThoseAuths() {
            // Narrowing {admin, ops, finance} to {ops} by predicate should equal resolving {ops}.
            assertThat(pushedVisDomain(ALL_AUTHS, isVisible(VIS_VAR, "ops")))
                .isEqualTo(noPredicate(OPS_ONLY));
        }

        @Test
        void compoundExpressionAdmittedOnlyWhenPredicateHoldsEveryToken() {
            Domain domain = pushedVisDomain(ALL_AUTHS, isVisible(VIS_VAR, "ops,finance"));

            assertThat(domain).isNotNull();
            assertThat(admits(domain, "ops")).isTrue();
            assertThat(admits(domain, "finance")).isTrue();
            assertThat(admits(domain, "ops&finance")).isTrue();
            assertThat(admits(domain, "admin")).as("admin file should be pruned").isFalse();
        }

        @Test
        void emptyPredicateAuthsLeavesOnlyUnrestricted() {
            Domain domain = pushedVisDomain(ALL_AUTHS, isVisible(VIS_VAR, ""));

            assertThat(domain).isNotNull();
            for (String vis : UNIVERSE) {
                assertThat(admits(domain, vis)).as(vis + " file should be pruned").isFalse();
            }
            assertAdmitsUnrestricted(domain);
        }

        @Test
        void multipleConjunctsAreIntersected() {
            Domain domain = pushedVisDomain(ALL_AUTHS,
                and(isVisible(VIS_VAR, "ops,finance"), isVisible(VIS_VAR, "finance")));

            assertThat(domain).isNotNull();
            assertThat(admits(domain, "finance")).isTrue();
            assertThat(admits(domain, "ops")).as("ops file should be pruned").isFalse();
            assertThat(admits(domain, "ops&finance")).as("ops&finance file should be pruned").isFalse();
            assertThat(admits(domain, "admin")).as("admin file should be pruned").isFalse();
        }

        @Test
        void conjunctIsHonoredAlongsideOtherPredicatesAndTheRowFilter() {
            // Realistic shape: the injected row filter, the user's predicate, and an unrelated conjunct.
            Domain domain = pushedVisDomain(ALL_AUTHS,
                and(isVisible(VIS_VAR, "admin,finance,ops"), isVisible(VIS_VAR, "ops"), idEqualsOne()));

            assertThat(domain).isNotNull();
            assertThat(admits(domain, "ops")).isTrue();
            assertThat(admits(domain, "admin")).as("admin file should be pruned").isFalse();
            assertThat(admits(domain, "finance")).as("finance file should be pruned").isFalse();
        }
    }

    /**
     * Cases that must not narrow or widen the domain. These pass today and must keep passing once
     * the predicate is honored.
     */
    @Nested
    class Guards {

        @Test
        void broaderPredicateDoesNotWidenDomain() {
            // Resolver grants {ops}; the query asks for everything.
            assertThat(pushedVisDomain(OPS_ONLY, isVisible(VIS_VAR, "admin,ops,finance")))
                .isEqualTo(noPredicate(OPS_ONLY));
        }

        @Test
        void disjunctionDoesNotNarrowDomain() {
            // is_visible(vis, 'ops') OR id = 1 can return an admin row with id 1, so no pruning on 'ops'.
            assertThat(pushedVisDomain(ALL_AUTHS, or(isVisible(VIS_VAR, "ops"), idEqualsOne())))
                .isEqualTo(noPredicate(ALL_AUTHS));
        }

        @Test
        void nonLiteralAuthsDoNotNarrowDomain() {
            // is_visible(vis, other): the auths vary per row, so nothing can be pruned on them.
            ConnectorExpression expr = isVisible(new Variable(VIS_VAR, VarcharType.VARCHAR),
                new Variable(OTHER_VAR, VarcharType.VARCHAR));
            assertThat(pushedVisDomain(ALL_AUTHS, expr)).isEqualTo(noPredicate(ALL_AUTHS));
        }

        @Test
        void predicateOnAnotherColumnDoesNotNarrowDomain() {
            assertThat(pushedVisDomain(ALL_AUTHS, isVisible(OTHER_VAR, "ops")))
                .isEqualTo(noPredicate(ALL_AUTHS));
        }

        @Test
        void pruningDisabledIgnoresPredicate() {
            assertThat(pushedVisDomain(ALL_AUTHS, false, isVisible(VIS_VAR, "ops"))).isNull();
        }
    }
}
