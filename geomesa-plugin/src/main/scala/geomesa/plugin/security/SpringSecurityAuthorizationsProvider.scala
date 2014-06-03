package geomesa.plugin.security

import geomesa.core.security.AuthorizationsProvider
import org.springframework.security.core.context.SecurityContextHolder
import org.springframework.security.core.Authentication
import org.apache.accumulo.core.security.Authorizations

/**
 * Implementation of AuthorizationsProvider that reads auths from spring security principal
 */
class SpringSecurityAuthorizationsProvider extends AuthorizationsProvider {

  override def getAuthorizations : Authorizations = {
    val option = Option(SecurityContextHolder.getContext.getAuthentication);
    option match {
      case Some(auth) => retrieveAuthorizationsFromPrincipal(auth)
      case None => new Authorizations
    }
  }

  def retrieveAuthorizationsFromPrincipal(auth: Authentication) : Authorizations = {
    println("AuthorizationsProvider::")
    println("\tprincipal: " + auth.getPrincipal)
    println("\tdetails: " + auth.getDetails)
    println("\tauthorities: " + auth.getAuthorities)
    println("\tcredentials: " + auth.getCredentials)
    new Authorizations
  }

}
