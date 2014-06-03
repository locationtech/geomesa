package geomesa.core.security

import org.apache.accumulo.core.security.Authorizations

/**
 * Test scala impl of authorizations provider
 */
class TestScalaAuthorizationsProvider extends AuthorizationsProvider {

  override def getAuthorizations : Authorizations = {
    new Authorizations
  }

}
