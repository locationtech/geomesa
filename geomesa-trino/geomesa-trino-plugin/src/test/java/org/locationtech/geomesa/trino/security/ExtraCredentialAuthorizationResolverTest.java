/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.trino.security;

import io.trino.spi.security.ConnectorIdentity;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class ExtraCredentialAuthorizationResolverTest {

    private static ConnectorIdentity identity(Map<String, String> extraCredentials) {
        return ConnectorIdentity.forUser("alice").withExtraCredentials(extraCredentials).build();
    }

    private static ExtraCredentialAuthorizationResolver resolver(Map<String, String> config) {
        return new ExtraCredentialAuthorizationResolver(config);
    }

    @Test
    void readsTokensFromDefaultCredential() {
        var r = resolver(Map.of());
        assertThat(r.authorizationsFor(identity(Map.of("auths", "basic,privileged"))))
            .containsExactlyInAnyOrder("basic", "privileged");
    }

    @Test
    void honorsConfiguredCredentialName() {
        var r = resolver(Map.of(ExtraCredentialAuthorizationResolver.CREDENTIAL_KEY, "x-auths"));
        assertThat(r.authorizationsFor(identity(Map.of("x-auths", "basic,privileged"))))
            .containsExactlyInAnyOrder("basic", "privileged");
    }

    @Test
    void readsPipeDelimitedTokens() {
        // The JDBC datastore joins tokens with pipes (spaces are rejected by the
        // Trino JDBC extraCredentials value validation).
        var r = resolver(Map.of());
        assertThat(r.authorizationsFor(identity(Map.of("auths", "basic|privileged"))))
            .containsExactlyInAnyOrder("basic", "privileged");
    }

    @Test
    void readsSpaceOrCommaDelimitedTokens() {
        // Robust to a mesh-injected header carrying a space or comma list too.
        var r = resolver(Map.of());
        assertThat(r.authorizationsFor(identity(Map.of("auths", "basic, privileged"))))
            .containsExactlyInAnyOrder("basic", "privileged");
    }

    @Test
    void whitespaceAndBlankTokensAreTrimmedAndDropped() {
        var r = resolver(Map.of());
        assertThat(r.authorizationsFor(identity(Map.of("auths", " basic , privileged ,,"))))
            .containsExactlyInAnyOrder("basic", "privileged");
    }

    @Test
    void missingCredentialFailsClosedEmpty() {
        var r = resolver(Map.of());
        assertThat(r.authorizationsFor(identity(Map.of("other", "basic,privileged")))).isEmpty();
    }

    @Test
    void secretGateHonorsAuthsWhenSecretMatches() {
        var r = resolver(Map.of(ExtraCredentialAuthorizationResolver.SECRET_KEY, "s3cr3t"));
        assertThat(r.authorizationsFor(identity(Map.of("secret", "s3cr3t", "auths", "basic privileged"))))
            .containsExactlyInAnyOrder("basic", "privileged");
    }

    @Test
    void secretGateFailsClosedWhenSecretWrong() {
        var r = resolver(Map.of(ExtraCredentialAuthorizationResolver.SECRET_KEY, "s3cr3t"));
        assertThat(r.authorizationsFor(identity(Map.of("secret", "nope", "auths", "basic privileged")))).isEmpty();
    }

    @Test
    void secretGateFailsClosedWhenSecretMissing() {
        var r = resolver(Map.of(ExtraCredentialAuthorizationResolver.SECRET_KEY, "s3cr3t"));
        assertThat(r.authorizationsFor(identity(Map.of("auths", "basic privileged")))).isEmpty();
    }

    @Test
    void emptyCredentialFailsClosedEmpty() {
        var r = resolver(Map.of());
        assertThat(r.authorizationsFor(identity(Map.of("auths", "")))).isEmpty();
    }

    @Test
    void tokensCarryingPairDelimitersAreDroppedFailClosed() {
        // ';'/':' survive the pipe/comma/whitespace split but delimit the extraCredentials
        // wire pairs — no legitimate producer can grant such a token (AuthTokens rejects
        // them at the source), so the resolver drops rather than honors it.
        var r = resolver(Map.of());
        assertThat(r.authorizationsFor(identity(Map.of("auths", "basic|FOO:BAR|privileged"))))
            .containsExactlyInAnyOrder("basic", "privileged");
    }
}
