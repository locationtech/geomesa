/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.trino.security;

import io.trino.spi.security.AccessDeniedException;
import io.trino.spi.security.HeaderAuthenticator;
import org.junit.jupiter.api.Test;

import java.security.Principal;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class TrustedPrincipalHeaderAuthenticatorTest {

    /** Arbitrary configured principal-header name used by these tests. */
    private static final String USER_HEADER = "X-Auth-User";

    /** Build a Headers view over a simple multi-valued map. */
    private static HeaderAuthenticator.Headers headers(Map<String, List<String>> map) {
        return name -> map.get(name);
    }

    /** Fail-closed authenticator (default: no X-Trino-User fallback). */
    private static Principal authenticate(Map<String, List<String>> map) {
        return new TrustedPrincipalHeaderAuthenticator(USER_HEADER).createAuthenticatedPrincipal(headers(map));
    }

    /** Authenticator with the X-Trino-User fallback explicitly enabled. */
    private static Principal authenticateWithFallback(Map<String, List<String>> map) {
        return new TrustedPrincipalHeaderAuthenticator(USER_HEADER, true).createAuthenticatedPrincipal(headers(map));
    }

    @Test
    void usesConfiguredUserHeaderAsPrincipal() {
        Principal p = authenticate(Map.of(
                USER_HEADER, List.of("alice"),
                "X-Trino-User", List.of("alice")));
        assertThat(p.getName()).isEqualTo("alice");
    }

    @Test
    void fallsBackToTrinoUserWhenFallbackEnabledAndPrincipalHeaderAbsent() {
        // Datastore (query-api workers) path: JDBC sets X-Trino-User, no gateway header.
        // Only honored when the fallback is explicitly enabled.
        Principal p = authenticateWithFallback(Map.of("X-Trino-User", List.of("wfs-bob")));
        assertThat(p.getName()).isEqualTo("wfs-bob");
    }

    @Test
    void fallsBackToTrinoUserWhenFallbackEnabledAndPrincipalHeaderBlank() {
        Principal p = authenticateWithFallback(Map.of(
                USER_HEADER, List.of("   "),
                "X-Trino-User", List.of("carol")));
        assertThat(p.getName()).isEqualTo("carol");
    }

    @Test
    void deniesTrinoUserWhenFallbackDisabled() {
        // Fail-closed default: a client-set X-Trino-User must NOT authenticate on its own.
        assertThatThrownBy(() -> authenticate(Map.of("X-Trino-User", List.of("mallory"))))
                .isInstanceOf(AccessDeniedException.class);
    }

    @Test
    void deniesWhenNoUserHeaderPresent() {
        assertThatThrownBy(() -> authenticate(Map.of("X-Other", List.of("x"))))
                .isInstanceOf(AccessDeniedException.class);
    }

    @Test
    void trimsAndTakesFirstNonBlankValue() {
        Principal p = authenticate(Map.of(USER_HEADER, List.of("  dave  ")));
        assertThat(p.getName()).isEqualTo("dave");
    }

    @Test
    void factoryNameAndConfigurableHeader() {
        TrustedPrincipalHeaderAuthenticatorFactory factory = new TrustedPrincipalHeaderAuthenticatorFactory();
        assertThat(factory.getName()).isEqualTo("trusted-principal-header");

        // The configured user-principal header is honored.
        HeaderAuthenticator auth = factory.create(Map.of("trusted-principal-header", "X-Custom-User"));
        Principal p = auth.createAuthenticatedPrincipal(headers(Map.of("X-Custom-User", List.of("erin"))));
        assertThat(p.getName()).isEqualTo("erin");
    }

    @Test
    void factoryRequiresUserPrincipalHeaderProperty() {
        TrustedPrincipalHeaderAuthenticatorFactory factory = new TrustedPrincipalHeaderAuthenticatorFactory();
        assertThatThrownBy(() -> factory.create(Map.of()))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void factoryDefaultsFailClosedForTrinoUser() {
        // No allow-trino-user-fallback key → fallback off → X-Trino-User alone is denied.
        HeaderAuthenticator auth = new TrustedPrincipalHeaderAuthenticatorFactory()
                .create(Map.of("trusted-principal-header", "X-Custom-User"));
        assertThatThrownBy(() -> auth.createAuthenticatedPrincipal(headers(Map.of("X-Trino-User", List.of("mallory")))))
                .isInstanceOf(AccessDeniedException.class);
    }

    @Test
    void factoryEnablesTrinoUserFallbackWhenConfigured() {
        HeaderAuthenticator auth = new TrustedPrincipalHeaderAuthenticatorFactory().create(Map.of(
                "trusted-principal-header", "X-Custom-User",
                "allow-trino-user-fallback", "true"));
        Principal p = auth.createAuthenticatedPrincipal(headers(Map.of("X-Trino-User", List.of("frank"))));
        assertThat(p.getName()).isEqualTo("frank");
    }
}
