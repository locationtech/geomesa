package geomesa.core.security

import org.apache.accumulo.core.security.Authorizations

/**
 * Default implementation of the AuthorizationsProvider that doesn't provide any authorizations
 */
class DefaultAuthorizationsProvider extends AuthorizationsProvider {

  var authorizations: Authorizations = new Authorizations

  override def getAuthorizations : Authorizations = { authorizations }
}
