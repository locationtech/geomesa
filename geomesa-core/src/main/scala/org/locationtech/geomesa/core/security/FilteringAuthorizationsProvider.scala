package org.locationtech.geomesa.core.security

import org.apache.accumulo.core.security.Authorizations
import org.locationtech.geomesa.core.data.AccumuloDataStoreFactory

import scala.collection.JavaConversions._

/**
 * AuthorizationsProvider that wraps another provider and ensures that the auths returned do not exceed a pre-set list
 */
class FilteringAuthorizationsProvider (val wrappedProvider: AuthorizationsProvider)
    extends AuthorizationsProvider {

  var filter: Option[Array[String]] = None

  override def getAuthorizations: Authorizations =
    filter match {
      case None => wrappedProvider.getAuthorizations
      case Some(_) =>  {
        val filtered = wrappedProvider.getAuthorizations.getAuthorizations.map(new String(_)).intersect(filter.get)
        new Authorizations(filtered:_*)
      }
    }

  override def configure(params: java.util.Map[String, java.io.Serializable]) {
    val authString = AccumuloDataStoreFactory.params.authsParam.lookUp(params).asInstanceOf[String]
    if (authString != null && !authString.isEmpty)
      filter = Option(authString.split(","))

    wrappedProvider.configure(params)
  }

}
