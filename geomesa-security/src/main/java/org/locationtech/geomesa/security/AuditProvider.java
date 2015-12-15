/*
 * Copyright (c) 2013-2015 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0 which
 * accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 */
package org.locationtech.geomesa.security;

import org.apache.accumulo.core.security.Authorizations;

import java.io.Serializable;
import java.util.Map;

/**
 * An interface to define passing audit information.
 */
public interface AuditProvider {

    public static final String AUDIT_PROVIDER_SYS_PROPERTY = "geomesa.audit.provider.impl";

    /**
     * Gets the current user.
     *
     * @return
     */
    public String getCurrentUserId();

    /**
     * Gets user details.
     *
     * @return
     */
    public Map<Object, Object> getCurrentUserDetails();

    /**
     * Configures this instance with parameters passed into the DataStoreFinder
     *
     * @param params
     */
    public void configure(Map<String, Serializable> params);
}
