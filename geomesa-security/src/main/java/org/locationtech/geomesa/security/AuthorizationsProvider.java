/***********************************************************************
 * Copyright (c) 2013-2023 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
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
     * @return authorizations
     */
    List<String> getAuthorizations();

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
