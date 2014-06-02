package geomesa.core.security;

import org.apache.accumulo.core.security.Authorizations;

/**
 * An abstract interface to define passing authorizations.
 */
public interface AuthorizationsProvider {

    /**
     * Gets the authorizations for the current context. This may change over time (e.g. in a multi-user environment), so the result should not be cached.
     *
     * @return
     */
    public Authorizations getAuthorizations();

}
