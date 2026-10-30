/***********************************************************************
 * Copyright (c) 2013-2026 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.security;

import java.util.Objects;
import java.util.Set;

/** Immutable delegated user, groups and auths, independent of transport header names. */
public final class DelegatedIdentity {
    private final String user;
    private final Set<String> groups;
    private final Set<String> auths;

    public DelegatedIdentity(String user, Set<String> groups, Set<String> auths) {
        Objects.requireNonNull(user, "Delegated user is required");
        if (user.isBlank()) {
            throw new IllegalArgumentException("Delegated user must not be blank");
        }
        this.user = user;
        this.groups = copyGrants(groups);
        this.auths = copyGrants(auths);
    }

    public String user() { return user; }
    public Set<String> groups() { return groups; }
    public Set<String> auths() { return auths; }

    private static Set<String> copyGrants(Set<String> grants) {
        var snapshot = Set.copyOf(grants);
        for (String grant : snapshot) {
            if (grant.isEmpty() || !grant.equals(grant.trim()) ||
                    grant.chars().anyMatch(c -> c == ',' || c == ';' || c < 32 || c > 126)) {
                throw new IllegalArgumentException("Invalid delegated grant");
            }
        }
        return snapshot;
    }

    @Override public String toString() { return "DelegatedIdentity[REDACTED]"; }
}
