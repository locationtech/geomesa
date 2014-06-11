package geomesa.core.security

import geomesa.core.data.AccumuloDataStoreFactory
import org.apache.accumulo.core.security.Authorizations

/**
 * Default implementation of the AuthorizationsProvider that doesn't provide any authorizations
 */
class DefaultAuthorizationsProvider extends AuthorizationsProvider {

  var authorizations: Authorizations = new Authorizations

  override def getAuthorizations: Authorizations = authorizations

  override def configure(params: java.util.Map[String, java.io.Serializable]) {
    val authString = AccumuloDataStoreFactory.params.authsParam.lookUp(params).asInstanceOf[String]
    if (authString == null || authString.isEmpty)
      authorizations = new Authorizations()
    else
      authorizations = new Authorizations(authString.split(","):_*)
  }

}
