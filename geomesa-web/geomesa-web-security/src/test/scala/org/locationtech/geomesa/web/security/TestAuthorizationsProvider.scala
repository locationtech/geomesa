package org.locationtech.geomesa.web.security

import java.io.Serializable
import java.util

import org.apache.accumulo.core.security.Authorizations
import org.locationtech.geomesa.security.AuthorizationsProvider
import org.springframework.security.authentication.TestingAuthenticationToken
import org.springframework.security.core.context.SecurityContextHolder

class TestAuthorizationsProvider extends AuthorizationsProvider {

  override def getAuthorizations: Authorizations = {
    import scala.collection.JavaConversions._
    val authentication = SecurityContextHolder.getContext.getAuthentication.asInstanceOf[TestingAuthenticationToken]
    val authorities = authentication.getAuthorities.map(_.getAuthority).toSeq
    new Authorizations(authorities: _*)
  }

  override def configure(params: util.Map[String, Serializable]): Unit = {}
}
