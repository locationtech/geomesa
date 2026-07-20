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
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

class FileAuthorizationResolverTest {

    private static ConnectorIdentity identity(String user, Set<String> groups) {
        return ConnectorIdentity.forUser(user).withGroups(groups).build();
    }

    @Test
    void userMappingResolves(@TempDir Path dir) throws Exception {
        Path f = dir.resolve("m.properties");
        Files.write(f, java.util.List.of("user.alice=basic,privileged"));
        var resolver = new FileAuthorizationResolver(f);
        assertThat(resolver.authorizationsFor(identity("alice", Set.of())))
            .containsExactlyInAnyOrder("basic", "privileged");
    }

    @Test
    void groupMappingResolves(@TempDir Path dir) throws Exception {
        Path f = dir.resolve("m.properties");
        Files.write(f, java.util.List.of("group.admins=basic,privileged"));
        var resolver = new FileAuthorizationResolver(f);
        assertThat(resolver.authorizationsFor(identity("bob", Set.of("admins"))))
            .containsExactlyInAnyOrder("basic", "privileged");
    }

    @Test
    void userAndGroupAuthsAreUnioned(@TempDir Path dir) throws Exception {
        Path f = dir.resolve("m.properties");
        Files.write(f, java.util.List.of("user.carol=basic", "group.admins-team=privileged"));
        var resolver = new FileAuthorizationResolver(f);
        assertThat(resolver.authorizationsFor(identity("carol", Set.of("admins-team"))))
            .containsExactlyInAnyOrder("basic", "privileged");
    }

    @Test
    void unknownIdentityYieldsEmptyAuths(@TempDir Path dir) throws Exception {
        Path f = dir.resolve("m.properties");
        Files.write(f, java.util.List.of("user.alice=basic,privileged"));
        var resolver = new FileAuthorizationResolver(f);
        assertThat(resolver.authorizationsFor(identity("nobody", Set.of("no-group")))).isEmpty();
    }

    @Test
    void whitespaceAndBlankTokensAreTrimmedAndDropped(@TempDir Path dir) throws Exception {
        Path f = dir.resolve("m.properties");
        Files.write(f, java.util.List.of("user.alice= basic , privileged ,,"));
        var resolver = new FileAuthorizationResolver(f);
        assertThat(resolver.authorizationsFor(identity("alice", Set.of())))
            .containsExactlyInAnyOrder("basic", "privileged");
    }

    @Test
    void pairDelimiterTokensAreHonored(@TempDir Path dir) throws Exception {
        Path f = dir.resolve("m.properties");
        Files.write(f, java.util.List.of("user.alice=basic,FOO;BAR,privileged"));
        var resolver = new FileAuthorizationResolver(f);
        assertThat(resolver.authorizationsFor(identity("alice", Set.of())))
            .containsExactlyInAnyOrder("basic", "FOO;BAR", "privileged");
    }

    @Test
    void missingFileFailsClosedEmpty(@TempDir Path dir) {
        var resolver = new FileAuthorizationResolver(dir.resolve("does-not-exist.properties"));
        assertThat(resolver.authorizationsFor(identity("alice", Set.of()))).isEmpty();
    }
}
