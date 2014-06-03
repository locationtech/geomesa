package geomesa.core;

import geomesa.core.security.AuthorizationsProvider;
import org.apache.accumulo.core.security.Authorizations;

/**
 * Created by elahrvivaz on 6/2/14.
 */
public class TestAuthorizationsProvider
        implements AuthorizationsProvider {
    @Override
    public Authorizations getAuthorizations() {
        return null;
    }
}
