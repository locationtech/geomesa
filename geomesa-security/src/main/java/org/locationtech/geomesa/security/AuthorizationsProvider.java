/***********************************************************************
 * Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.security;

import org.apache.accumulo.core.security.Authorizations;

import java.io.Serializable;
import java.util.Map;

/**
 * An interface to define passing authorizations.
 */
public interface AuthorizationsProvider {

    public static final String AUTH_PROVIDER_SYS_PROPERTY = "geomesa.auth.provider.impl";

    /**
     * Gets the authorizations for the current context. This may change over time (e.g. in a multi-user environment), so the result should not be cached.
     *
     * @return
     */
    public Authorizations getAuthorizations();

    /**
     * Configures this instance with parameters passed into the DataStoreFinder
     *
     * @param params
     */
    public void configure(Map<String, Serializable> params);
}
