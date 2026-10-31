/***********************************************************************
 * Copyright (c) 2013-2026 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.security;

import java.util.Collection;
import java.util.Objects;
import java.util.Set;

/** Immutable delegated user, groups and auths, independent of transport header names. */
public final class DelegatedIdentity {

    public static final String ANONYMOUS_USER = "anonymous";

    private final String user;
    private final Set<String> groups;
    private final Set<String> auths;

    public DelegatedIdentity(String user, Collection<String> groups, Collection<String> auths) {
        Objects.requireNonNull(user, "Delegated user is required");
        if (user.isBlank()) {
            throw new IllegalArgumentException("Delegated user must not be blank");
        }
        this.user = user;
        this.groups = groups == null ? Set.of() : Set.copyOf(groups);
        this.auths = auths == null ? Set.of() : Set.copyOf(auths);
    }

    public String user() { return user; }
    public Set<String> groups() { return groups; }
    public Set<String> auths() { return auths; }

    @Override
    public String toString() {
        return "DelegatedIdentity[user=" + user + ", groups=" + groups + ", auths=" + auths + "]";
    }

    public static DelegatedIdentity anonymous(Collection<String> auths) {
        return new DelegatedIdentity(ANONYMOUS_USER, Set.of(), auths);
    }
}
