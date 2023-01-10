/***********************************************************************
 * Copyright (c) 2013-2024 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.security;

<<<<<<< HEAD
=======
import java.util.ArrayList;
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> e74fa3f690 (GEOMESA-3254 Add Bloop build support)
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 58d14a257 (GEOMESA-3254 Add Bloop build support)
>>>>>>> fa60953a42 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
=======
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)
>>>>>>> 7564665969 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> e74fa3f690 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)
>>>>>>> 9e49c1aac7 (GEOMESA-3254 Add Bloop build support)
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
<<<<<<< HEAD
    @Deprecated
    static AuthorizationsProvider apply(Map<String, ?> params, List<String> authorizations) {
        return AuthUtils.getProvider(params, authorizations);
=======
    static AuthorizationsProvider apply(Map<String, ?> params, List<String> authorizations) {
        // we wrap the authorizations provider in one that will filter based on the configured max auths
        List<AuthorizationsProvider> providers = new ArrayList<>();
        for (AuthorizationsProvider p: ServiceLoader.load(AuthorizationsProvider.class)) {
            providers.add(p);
        }

        AuthorizationsProvider provider = package$.MODULE$.AuthProviderParam().lookup(params);

        if (provider == null) {
           String name = package$.MODULE$.GEOMESA_AUTH_PROVIDER_IMPL().get();
           if (name == null) {
               if (providers.isEmpty()) {
                   provider = new DefaultAuthorizationsProvider();
               } else if (providers.size() == 1) {
                   provider = providers.get(0);
               } else {
                   String prop = package$.MODULE$.GEOMESA_AUTH_PROVIDER_IMPL().property();
                   StringJoiner options = new StringJoiner(", ");
                   for (AuthorizationsProvider p : providers) {
                       options.add(p.getClass().getName());
                   }
                   throw new IllegalStateException(
                         "Found multiple AuthorizationsProvider implementations: " + options.toString()
                         + ". Please specify the one to use with the system property '" + prop + "'");
               }
           } else if (DefaultAuthorizationsProvider.class.getName().equals(name)) {
               provider = new DefaultAuthorizationsProvider();
           } else {
               for (AuthorizationsProvider p : providers) {
                   if (p.getClass().getName().equals(name)) {
                       provider = p;
                       break;
                   }
               }
               if (provider == null) {
                   String prop = package$.MODULE$.GEOMESA_AUTH_PROVIDER_IMPL().property();
                   StringJoiner options = new StringJoiner(", ");
                   for (AuthorizationsProvider p : providers) {
                       options.add(p.getClass().getName());
                   }
                   throw new IllegalArgumentException(
                         "The service provider class '" + name + "' specified by '" + prop + "' could not be " +
                         "loaded. Available providers are: " + options.toString());
               }
           }
        }

        provider = new FilteringAuthorizationsProvider(provider);

        // update the authorizations in the parameters and then configure the auth provider
        // we copy the map so as not to modify the original
        Map<String, Object> modifiedParams = new java.util.HashMap<>(params);
        StringJoiner auths = new StringJoiner(",");
        for (String auth: authorizations) {
            auths.add(auth);
        }
        modifiedParams.put(package$.MODULE$.AuthsParam().key, auths.toString());
        provider.configure(modifiedParams);

        return provider;
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> e74fa3f690 (GEOMESA-3254 Add Bloop build support)
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 58d14a257 (GEOMESA-3254 Add Bloop build support)
>>>>>>> fa60953a42 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
=======
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)
>>>>>>> 7564665969 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> e74fa3f690 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)
>>>>>>> 9e49c1aac7 (GEOMESA-3254 Add Bloop build support)
    }
}
