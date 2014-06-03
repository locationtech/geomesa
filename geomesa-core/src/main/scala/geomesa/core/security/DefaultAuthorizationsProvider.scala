package geomesa.core.security

import org.apache.accumulo.core.security.Authorizations

/**
 * Default implementation of the AuthorizationsProvider that doesn't provide any authorizations
 */
class DefaultAuthorizationsProvider(authorizations: Authorizations = new Authorizations) extends AuthorizationsProvider {

  override def getAuthorizations : Authorizations = { authorizations }

}
