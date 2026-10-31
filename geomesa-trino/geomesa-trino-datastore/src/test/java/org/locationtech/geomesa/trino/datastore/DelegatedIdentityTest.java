/***********************************************************************
 * Copyright (c) 2013-2026 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.trino.datastore;

import org.junit.jupiter.api.Test;
import org.locationtech.geomesa.security.AuthorizationsProvider;
import org.locationtech.geomesa.security.DelegatedIdentity;

import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.Callable;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class DelegatedIdentityTest {

    private static class ThreadLocalAuthProvider implements AuthorizationsProvider {

        public static ThreadLocal<DelegatedIdentity> identities = new ThreadLocal<>();

        @Override
        public List<String> getAuthorizations() {
            var user = currentUser();
            return user == null ? null : List.copyOf(user.auths());
        }

        @Override
        public DelegatedIdentity currentUser() {
            return identities.get();
        }

        @Override
        public void configure(Map<String, ?> params) {
        }
    }

    private static class Store extends TrinoDataStore {
        final List<Properties> calls = new ArrayList<>();

        Store() {
            this("x-test-user", "x-test-groups", "x-test-auths");
        }

        Store(String userHeader, String groupsHeader, String authsHeader) {
            super("trino", 8080, "spatial_iceberg", "spatial",
                new ThreadLocalAuthProvider(), "geomesa", null, null);
            configureDelegation(true, "jdbc:trino://trino:8443/spatial_iceberg/spatial?SSL=true",
                userHeader, groupsHeader, authsHeader);
        }

        @Override protected Connection openDelegatedConnection(String url, Properties props) {
            assertThat(url).isEqualTo("jdbc:trino://trino:8443/spatial_iceberg/spatial?SSL=true");
            calls.add(props);
            return (Connection) Proxy.newProxyInstance(Connection.class.getClassLoader(), new Class[]{Connection.class},
                (proxy, method, args) -> null);
        }
    }

    private static Connection withIdentity(DelegatedIdentity identity, List<String> auths, Callable<Connection> action) {
        var existing = ThreadLocalAuthProvider.identities.get();
        ThreadLocalAuthProvider.identities.set(new DelegatedIdentity(identity.user(), identity.groups(), auths));
        try {
            return action.call();
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            if (existing == null) {
                ThreadLocalAuthProvider.identities.remove();
            } else {
                ThreadLocalAuthProvider.identities.set(existing);
            }
        }
    }

    @Test void headerNamesComeFromConfigurationWithoutChangingTheIdentity() {
        var store = new Store("Application-User", "Application-Groups", "Application-Auths");
        withIdentity(new DelegatedIdentity("alice", Set.of("analysts"), Set.of("user")), List.of("user"),
            () -> store.connect(List.of("user")));
        assertThat(store.calls.get(0).getProperty("extraHeaders"))
            .contains("application-user:alice", "application-groups:analysts", "application-auths:user")
            .doesNotContain("x-test-");
    }

    @Test void delegationRequiresThreeDistinctValidConfiguredHeaderNames() {
        assertThatThrownBy(() -> new Store(null, "groups", "auths")).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new Store("user", null, "auths")).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new Store("user", "groups", null)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new Store("user", "groups", "User"))
            .hasMessageContaining("must be distinct");
        assertThatThrownBy(() -> new Store("bad:name", "groups", "auths"))
            .isInstanceOf(IllegalArgumentException.class);
    }

    @Test void factoryReadsConfiguredHeaderNamesAndRequiresThemOnlyForDelegation() throws Exception {
        var factory = new TrinoDataStoreFactory();
        factory.createDataStore(Map.of("trino.host", "127.0.0.1", "trino.port", 1)).dispose();
        assertThatThrownBy(() -> factory.createDataStore(Map.of("trino.host", "127.0.0.1", "trino.port", 1,
            "trino.delegation.enabled", true))).isInstanceOf(IllegalArgumentException.class);
        var store = (TrinoDataStore) factory.createDataStore(Map.of(
            "trino.host", "127.0.0.1", "trino.port", 1, "trino.delegation.enabled", true,
            "trino.delegation-url", "jdbc:trino://127.0.0.1:1/spatial_iceberg/spatial?SSL=true",
            "trino.delegation.user-header", "Factory-User",
            "trino.delegation.groups-header", "Factory-Groups",
            "trino.delegation.auths-header", "Factory-Auths"));
        try {
            withIdentity(new DelegatedIdentity("alice", Set.of(), Set.of()),
                List.of(), () -> store.connect(List.of())).close();
        } finally {
            store.dispose();
        }
    }

    @Test void equalAuthsHaveDistinctUsersGroupsAndConnections() {
        var store = new Store();
        var alice = withIdentity(new DelegatedIdentity("alice", Set.of("team:one", "team-two"), Set.of("user", "admin")), List.of("user"),
            () -> store.connect(List.of("user")));
        var bob = withIdentity(new DelegatedIdentity("bob", Set.of("other"), Set.of("user")), List.of("user"),
            () -> store.connect(List.of("user")));
        assertThat(alice).isNotSameAs(bob);
        assertThat(store.calls.get(0).getProperty("user")).isEqualTo("alice");
        assertThat(store.calls.get(1).getProperty("user")).isEqualTo("bob");
        assertThat(store.calls.get(0).getProperty("extraHeaders"))
            .contains("x-test-user:alice", "x-test-groups:team-two,team:one", "x-test-auths:user")
            .doesNotContain("admin");
        assertThat(store.calls.get(0)).doesNotContainKey("extraCredentials");
        assertThat(ThreadLocalAuthProvider.identities.get()).isNull();
    }

    @Test void metadataUsesJobIdentityInScope() throws Exception {
        var store = new Store();
        withIdentity(new DelegatedIdentity("alice", Set.of("analysts"), Set.of("user", "admin")),
            List.of("user"), store::connect);
        assertThat(store.calls.get(0).getProperty("user")).isEqualTo("alice");
        assertThat(store.calls.get(0).getProperty("extraHeaders"))
            .contains("x-test-groups:analysts", "x-test-auths:user").doesNotContain("admin");
        assertThatThrownBy(() -> store.connect()).hasMessage("Delegated identity is missing");
    }

    @Test void missingQueryIdentityFailsWithoutConnecting() {
        var store = new Store();
        assertThatThrownBy(() -> store.connect(List.of("user"))).hasMessage("Delegated identity is missing");
        assertThat(store.calls).isEmpty();
        assertThat(ThreadLocalAuthProvider.identities.get()).isNull();
    }

    @Test void unrepresentableUserFailsWithoutConnecting() {
        var store = new Store();
        assertThatThrownBy(() -> withIdentity(new DelegatedIdentity("alice;invalid", Set.of(), Set.of()),
            List.of(), () -> store.connect(List.of())))
            .cause().hasMessage("Delegated user 'alice;invalid' cannot be represented in JDBC headers");
        assertThat(store.calls).isEmpty();
    }

    @Test void emptyProfileGrantsRemainEmptyAndUseDedicatedConnections() {
        var store = new Store();
        var identity = new DelegatedIdentity("alice", Set.of(), Set.of());
        var one = withIdentity(identity, List.of(), () -> store.connect(List.of()));
        var two = withIdentity(identity, List.of(), () -> store.connect(List.of()));
        assertThat(one).isNotSameAs(two);
        assertThat(store.calls).hasSize(2);
        assertThat(store.calls.get(0).getProperty("extraHeaders")).isEqualTo("x-test-user:alice");
        assertThat(store.calls.get(0)).doesNotContainKey("extraCredentials");
    }

    @Test void unrepresentableGroupsFailWithoutConnectingAndIdentifyTheGrant() {
        var store = new Store();
        for (String grant : List.of("", "one,two", "one;two", " team", "team ", "two teams", "équipe", "a\nb", "a\u007fb")) {
            var identity = new DelegatedIdentity("alice", Set.of(grant), Set.of());
            assertThatThrownBy(() -> withIdentity(identity, List.of(), () -> store.connect(List.of())))
                .cause().isInstanceOf(SQLException.class)
                .hasMessage("Delegated grant '" + grant + "' cannot be represented in JDBC headers");
        }
        assertThat(store.calls).isEmpty();
    }

    @Test void unrepresentableAuthsFailWithoutConnectingAndIdentifyTheGrant() {
        var store = new Store();
        for (String grant : List.of("", "one,two", "one;two", " team", "team ", "two teams", "équipe", "a\nb", "a\u007fb")) {
            var identity = new DelegatedIdentity("alice", Set.of(), Set.of(grant));
            for (Callable<Connection> connect : List.<Callable<Connection>>of(
                    () -> store.connect(List.of(grant)), () -> store.connect(null), store::connect)) {
                assertThatThrownBy(() -> withIdentity(identity, List.of(grant), connect))
                    .cause().isInstanceOf(SQLException.class)
                    .hasMessage("Delegated grant '" + grant + "' cannot be represented in JDBC headers");
            }
        }
        assertThat(store.calls).isEmpty();
    }

    @Test void delegatedPropertiesIncludingEmptyGrantsAreAcceptedByTheRealJdbcDriver() throws Exception {
        var store = new TrinoDataStore("127.0.0.1", 1, "spatial_iceberg", "spatial", new ThreadLocalAuthProvider(), "geomesa", null, null);
        store.configureDelegation(true, "jdbc:trino://127.0.0.1:1/spatial_iceberg/spatial?SSL=true", "x-test-user", "x-test-groups", "x-test-auths");
        withIdentity(new DelegatedIdentity("alice", Set.of("group:allowed"), Set.of("user")),
            List.of("user"), () -> store.connect(List.of("user"))).close();
        withIdentity(new DelegatedIdentity("alice", Set.of(), Set.of()),
            List.of(), () -> store.connect(List.of())).close();
    }

    @Test void driverErrorsDoNotExposeIdentityProperties() {
        var store = new TrinoDataStore("127.0.0.1", 1, "spatial_iceberg", "spatial", new ThreadLocalAuthProvider(), "geomesa", null, null);
        store.configureDelegation(true, "jdbc:trino://127.0.0.1:1/spatial_iceberg/spatial?unknownProperty=value", "x-test-user", "x-test-groups", "x-test-auths");
        assertThatThrownBy(() -> withIdentity(new DelegatedIdentity("alice", Set.of("analysts"), Set.of("user")), List.of("user"), () -> store.connect(List.of("user"))))
            .cause().isInstanceOf(SQLException.class)
            .hasMessage("Unable to establish a connection with delegated identity").hasNoCause();
    }
}
