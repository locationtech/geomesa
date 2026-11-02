/***********************************************************************
 * Copyright (c) 2013-2026 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.security;

import java.util.List;
import java.util.Map;

/**
 * An interface to define passing authorizations.
 */
public interface AuthorizationsProvider {

    String AUTH_PROVIDER_SYS_PROPERTY = "geomesa.auth.provider.impl";

    /**
     * Gets the authorizations for the current context. This may change over time (e.g. in a multi-user environment),
     * so the result should not be cached.
     *
     * @return authorizations, not null
     */
    List<String> getAuthorizations();

    /**
     * Gets the current user, with groups and auths. This may change over time (e.g. in a multi-user environment),
     * so the result should not be cached. The authorizations *must* be the same as returned from `getAuthorizations`
     * (for a given call point). If there is no actual end user, a system-level user should be returned.
     * <br/>
     * The default implementation returns an anonymous user, with no groups, and the auths returned from
     * `getAuthorizations`.
     *
     * @return current user, not null
     */
    default DelegatedIdentity currentUser() {
        return DelegatedIdentity.anonymous(getAuthorizations());
    }

    /**
     * Configures this instance with parameters passed into the DataStoreFinder
     *
     * @param params parameters
     */
    void configure(Map<String, ?> params);

    /**
     * Moved to `org.locationtech.geomesa.security.AuthUtils.getProvider` for Java 17 compatibility
     */
    @Deprecated
    static AuthorizationsProvider apply(Map<String, ?> params, List<String> authorizations) {
        return AuthUtils.getProvider(params, authorizations);
    }
}
