/***********************************************************************
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.security;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

/**
 * An interface to define passing authorizations.
 */
public interface AuthorizationsProvider {

    static final String AUTH_PROVIDER_SYS_PROPERTY = "geomesa.auth.provider.impl";

    /**
     * Gets the authorizations for the current context. This may change over time (e.g. in a multi-user environment),
     * so the result should not be cached.
     *
     * @return
     */
    List<String> getAuthorizations();

    /**
     * Configures this instance with parameters passed into the DataStoreFinder
     *
     * @param params
     */
    void configure(Map<String, Serializable> params);
}
