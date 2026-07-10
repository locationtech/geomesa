/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.trino.security;

import io.trino.spi.security.ConnectorIdentity;

import java.util.Set;

/**
 * Maps a Trino session identity to the set of authorization tokens it holds
 * (e.g. {@code basic}, {@code privileged}), used to build the row-level
 * visibility filter. The pluggable seam: the built-in implementation is
 * {@link FileAuthorizationResolver}; an external lookup (LDAP/REST/IdP claims)
 * can be supplied via the {@code geomesa.security.auth-resolver} catalog
 * property naming a class with a {@code (Map<String,String> config)} constructor.
 *
 * <p>An unknown or unmapped identity must resolve to an empty set — that is
 * fail-closed (the row filter then admits only unrestricted rows).
 */
public interface AuthorizationResolver {

    /**
     * Authorizations for the given identity; empty set if none (fail-closed).
     *
     * @param identity the Trino session identity to resolve
     * @return authorization tokens held by the identity; empty set if none
     */
    Set<String> authorizationsFor(ConnectorIdentity identity);
}
