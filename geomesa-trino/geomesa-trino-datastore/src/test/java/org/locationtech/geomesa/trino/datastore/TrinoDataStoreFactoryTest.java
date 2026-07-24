/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.trino.datastore;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class TrinoDataStoreFactoryTest {

    private static Map<String, Object> baseParams() {
        Map<String, Object> params = new HashMap<>();
        params.put("trino.host", "localhost");
        params.put("trino.port", 8080);
        return params;
    }

    @Test
    void noSecurityParamsStillYieldsProvider() throws IOException {
        // The provider is resolved unconditionally (like Accumulo/FSDS) so a
        // server-installed per-request provider is honored. With nothing
        // configured and no provider on the classpath, it's a Default over empty
        // auths — fail-closed (vis-bearing tables show only unrestricted rows).
        TrinoDataStore store =
            (TrinoDataStore) new TrinoDataStoreFactory().createDataStore(baseParams());
        assertThat(store.authProvider()).isNotNull();
        assertThat(store.authProvider().getAuthorizations()).isEmpty();
    }

    @Test
    void authsParamYieldsConfiguredAuths() throws IOException {
        Map<String, Object> params = baseParams();
        params.put("geomesa.security.auths", "admin,user");
        TrinoDataStore store =
            (TrinoDataStore) new TrinoDataStoreFactory().createDataStore(params);
        assertThat(store.authProvider()).isNotNull();
        assertThat(store.authProvider().getAuthorizations())
            .containsExactly("admin", "user");
    }

    @Test
    void authsAreTrimmedAndBlanksDropped() throws IOException {
        Map<String, Object> params = baseParams();
        params.put("geomesa.security.auths", " admin, user,,ops ");
        TrinoDataStore store =
            (TrinoDataStore) new TrinoDataStoreFactory().createDataStore(params);
        assertThat(store.authProvider().getAuthorizations())
            .containsExactly("admin", "user", "ops");
    }

    @Test
    void explicitProviderParamIsUsed() throws IOException {
        Map<String, Object> params = baseParams();
        params.put("geomesa.security.auths.provider",
            new org.locationtech.geomesa.security.AuthorizationsProvider() {
                @Override public java.util.List<String> getAuthorizations() {
                    return java.util.List.of("ops");
                }
                @Override public void configure(java.util.Map<String, ?> p) {}
            });
        TrinoDataStore store =
            (TrinoDataStore) new TrinoDataStoreFactory().createDataStore(params);
        assertThat(store.authProvider().getAuthorizations()).containsExactly("ops");
    }
}
