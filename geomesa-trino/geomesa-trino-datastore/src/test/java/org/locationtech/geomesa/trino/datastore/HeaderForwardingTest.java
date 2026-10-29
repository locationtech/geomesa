/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.trino.datastore;

import org.junit.jupiter.api.Test;
import org.locationtech.geomesa.security.ThreadLocalAuthorizationsProvider;
import org.springframework.http.HttpHeaders;
import scala.runtime.AbstractFunction0;
import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import static org.assertj.core.api.Assertions.*;

class HeaderForwardingTest {
    private static HttpHeaders toHttpHeaders(Map<String, String> values) {
        var headers = new HttpHeaders();
        values.forEach(headers::set);
        return headers;
    }
    private static String encoded(String value) {
        return java.util.Base64.getEncoder().encodeToString(value.getBytes(java.nio.charset.StandardCharsets.US_ASCII));
    }
    private static class Store extends TrinoDataStore {
        final List<Properties> calls = new ArrayList<>();
        Store() {
            super("trino", 8080, "spatial_iceberg", "spatial", null, "geomesa", null, null);
            configureHeaderForwarding("Authorization,X-Trace-Context", "jdbc:trino://gateway:443/spatial_iceberg/spatial?SSL=true");
        }
        @Override protected Connection openForwardedConnection(String url, Properties props) {
            assertThat(url).contains("gateway:443");
            calls.add(props);
            return (Connection) Proxy.newProxyInstance(Connection.class.getClassLoader(), new Class[]{Connection.class},
                (proxy, method, args) -> null);
        }
    }
    private static Connection query(Store store, Map<String, String> values) {
        return query(store, toHttpHeaders(values));
    }
    private static Connection query(Store store, HttpHeaders headers) {
        return ThreadLocalAuthorizationsProvider.withRequestHeaders(headers, new AbstractFunction0<Connection>() {
            @Override public Connection apply() {
                try { return store.connect(List.of("U")); }
                catch (java.sql.SQLException e) { throw new RuntimeException(e); }
            }
        });
    }
    @Test void equalGrantsNeverReuseAnotherRequestsHeadersAndOnlySelectedHeadersLeave() {
        var store = new Store();
        var one = query(store, Map.of("Authorization", "Bearer alice", "X-Trace-Context", "one", "X-Unselected", "private"));
        var two = query(store, Map.of("Authorization", "Basic bob", "X-Trace-Context", "two"));
        assertThat(one).isNotSameAs(two);
        assertThat(store.calls).hasSize(2);
        assertThat(store.calls.get(0).getProperty("extraHeaders")).contains("x-geomesa-forwarded-authorization:" + encoded("Bearer alice"), "x-geomesa-forwarded-x-trace-context:" + encoded("one"))
            .doesNotContain("private");
        assertThat(store.calls.get(1).getProperty("extraHeaders")).contains("x-geomesa-forwarded-authorization:" + encoded("Basic bob"), "x-geomesa-forwarded-x-trace-context:" + encoded("two"));
        assertThat(ThreadLocalAuthorizationsProvider.requestHeaders().isEmpty()).isTrue();
    }
    @Test void missingHeadersFailAndDelimiterValuesRemainOpaque() {
        var store = new Store();
        assertThatThrownBy(() -> store.connect(List.of("U"))).hasMessageContaining("forwarded header is missing");
        assertThat(store.calls).isEmpty();
        query(store, Map.of("Authorization", "Bearer abc;Other:literal", "X-Trace-Context", "one"));
        assertThat(store.calls.get(0).getProperty("extraHeaders")).contains(encoded("Bearer abc;Other:literal"))
            .doesNotContain("Other:literal");
    }
    @Test void rotatedOpaqueValuesOpenFreshConnections() {
        var store = new Store();
        query(store, Map.of("Authorization", "old", "X-Trace-Context", "one"));
        query(store, Map.of("Authorization", "new", "X-Trace-Context", "one"));
        assertThat(store.calls).hasSize(2);
        assertThat(store.calls.get(1).getProperty("extraHeaders")).contains("x-geomesa-forwarded-authorization:" + encoded("new"));
    }
    @Test void rejectsTransportControlledAndMalformedConfiguredHeaderNames() {
        var store = new Store();
        for (String name : List.of("Host", "Connection", "Invalid Name")) {
            assertThatThrownBy(() -> store.configureHeaderForwarding(name, null))
                .isInstanceOf(IllegalArgumentException.class);
        }
    }
    @Test void rejectsMultipleOrInvalidValuesAtTheForwardingBoundary() {
        var store = new Store();
        var duplicate = toHttpHeaders(Map.of("Authorization", "one", "X-Trace-Context", "trace"));
        duplicate.add("authorization", "two");
        assertThatThrownBy(() -> query(store, duplicate)).hasMessageContaining("invalid value");
        for (String value : List.of("secret\r\nInjected: yes", "non-ASCII \u00e9")) {
            assertThatThrownBy(() -> query(store, Map.of("Authorization", value, "X-Trace-Context", "trace")))
                .hasMessageContaining("invalid value");
        }
        assertThat(store.calls).isEmpty();
    }
    @Test void preservesLeadingAndTrailingWhitespaceInOpaqueValues() {
        var store = new Store();
        query(store, Map.of("Authorization", " opaque credential ", "X-Trace-Context", "trace"));
        assertThat(store.calls.get(0).getProperty("extraHeaders")).contains(encoded(" opaque credential "));
    }
    @Test void driverPropertyErrorsDoNotRetainSecretValuesOrTheirCause() {
        var store = new TrinoDataStore("trino", 8080, "spatial_iceberg", "spatial", null, "geomesa", null, null);
        store.configureHeaderForwarding("Authorization", "jdbc:trino://127.0.0.1:1/spatial_iceberg/spatial?unknownProperty=value");
        try {
            ThreadLocalAuthorizationsProvider.withRequestHeaders(toHttpHeaders(Map.of("Authorization", "Bearer private")),
                new AbstractFunction0<Connection>() {
                    @Override public Connection apply() {
                        try { return store.connect(List.of("U")); }
                        catch (java.sql.SQLException e) { throw new RuntimeException(e); }
                    }
                });
            fail("Expected a driver property error");
        } catch (RuntimeException e) {
            assertThat(e.getCause()).isInstanceOf(java.sql.SQLException.class)
                .hasMessage("Unable to establish a connection with forwarded request headers").hasNoCause();
        }
    }

}
