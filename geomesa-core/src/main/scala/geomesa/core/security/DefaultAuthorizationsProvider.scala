package geomesa.core.security

import org.apache.accumulo.core.security.Authorizations

/**
 * Default implementation of the AuthorizationsProvider that doesn't provide any authorizations
 */
class DefaultAuthorizationsProvider extends AuthorizationsProvider {

  override def getAuthorizations : Authorizations = { new Authorizations }

}
