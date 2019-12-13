/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.utils.audit;

import org.locationtech.geomesa.utils.conf.GeoMesaSystemProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.Iterator;
import java.util.Map;
import java.util.ServiceLoader;

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

    /**
     * Loader
     */
    public static class Loader {

        private static final Logger logger = LoggerFactory.getLogger(Loader.class);

        private Loader() {}

        public static AuditProvider load(Map<String, Serializable> params) {

            Iterator<AuditProvider> providers = ServiceLoader.load(AuditProvider.class).iterator();

            // if the user specifies an auth provider to use, try to use that impl
            String specified = GeoMesaSystemProperties.getProperty(AuditProvider.AUDIT_PROVIDER_SYS_PROPERTY);
            if (specified != null) {
                if (specified.equals("org.locationtech.geomesa.plugin.security.SpringAuditProvider")) {
                    logger.warn("org.locationtech.geomesa.plugin.security.SpringAuditProvider is deprecated;" +
                                "switching to org.locationtech.geomesa.security.SpringAuditProvider");
                    specified = "org.locationtech.geomesa.security.SpringAuditProvider";
                }
                while (providers.hasNext()) {
                    AuditProvider provider = providers.next();
                    if (specified.equals(provider.getClass().getName())) {
                        provider.configure(params);
                        return provider;
                    }
                }
                throw new RuntimeException("Could not load audit provider " + specified);
            }

            if (providers.hasNext()) {
                AuditProvider provider = providers.next();
                if (providers.hasNext()) {
                    StringBuilder all = new StringBuilder(provider.getClass().getName());
                    while (providers.hasNext()) {
                        all.append(", ").append(providers.next().getClass().getName());
                    }
                    throw new IllegalStateException(
                            "Found multiple AuditProvider implementations. Please specify the one to use with the system " +
                            "property '" + AUDIT_PROVIDER_SYS_PROPERTY + "' :: " + all);
                } else {
                    provider.configure(params);
                    return provider;
                }
            } else {
                return null;
            }
        }
    }
}