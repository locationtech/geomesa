package org.locationtech.geomesa.core;

import org.locationtech.geomesa.core.security.AuthorizationsProvider;
import org.apache.accumulo.core.security.Authorizations;

import java.io.Serializable;
import java.util.Map;

/**
 * Test authorizations provider that doesn't use any auths
 */
public class TestAuthorizationsProvider
        implements AuthorizationsProvider {
    @Override
    public Authorizations getAuthorizations() {
        return null;
    }

    @Override
    public void configure(Map<String, Serializable> params) {

    }
}
