package geomesa.plugin.security

import geomesa.core.security.AuthorizationsProvider
import org.springframework.security.core.context.SecurityContextHolder
import org.springframework.security.core.Authentication
import org.apache.accumulo.core.security.Authorizations
import java.util.Properties
import javax.naming.ldap.{LdapContext, InitialLdapContext}
import javax.naming.directory.SearchControls
import com.typesafe.scalalogging.slf4j.Logging

/**
 * Implementation of AuthorizationsProvider that reads auths from ldap based on the spring security principal
 */
class GeoServerLdapAuthorizationsProvider extends AuthorizationsProvider with Logging {

  import GeoServerLdapAuthorizationsProvider._

  val environment: Properties = {
    // load the properties from the props file on the classpath
    val env = new Properties
    val inputStream = getClass
                      .getClassLoader
                      .getResourceAsStream(PROPS_FILE)
    env.load(inputStream)
    inputStream.close
    env
  }

  // the ldap node to start the query from
  val searchRoot = environment.getProperty(SEARCH_ROOT)

  // the query that will be applied to find the user's record - the symbol {} will be replaced with the user's cn
  val searchFilter = environment.getProperty(SEARCH_FILTER)

  // the ldap attribute that holds the comma-delimited authorizations for the user
  val authsAttribute = environment.getProperty(AUTHS_ATTRIBUTE)

  // Create the search controls for querying ldap
  val searchControls = new SearchControls
  // specify the fields we want returned - the auths in this case
  searchControls.setReturningAttributes(Array("cn", authsAttribute))
  // limit the search to a subtree scope from the search root
  searchControls.setSearchScope(SearchControls.SUBTREE_SCOPE);

  override def getAuthorizations: Authorizations = {
    // if there is an authenticated spring object, try to retrieve the auths from that, otherwise use empty auths
    val option = Option(SecurityContextHolder.getContext.getAuthentication);
    option match {
      case Some(auth) => retrieveAuthorizationsFromPrincipal(auth)
      case None       => new Authorizations
    }
  }

  /**
   * Retrieves the authorizations from the spring principal
   *
   * @param auth
   * @return
   */
  def retrieveAuthorizationsFromPrincipal(auth: Authentication): Authorizations = {
    val principal = auth.getPrincipal

    // the principal should be a string containing the cn of the user
    if (!principal.isInstanceOf[String]) {
      logger
      .debug("AuthorizationsProvider:: found unexpected principal type of {} - expected String",
              principal.getClass)
      return new Authorizations
    }
    val cn = principal.asInstanceOf[String]

    // initialize the auth string - we will set it below if the ldap connection/query succeeds
    var authString = None: Option[String]

    // the ldap context for querying ldap
    var context: LdapContext = null

    try {
      // create the context - this may throw various exceptions for connections, authentication, etc {
      context = new InitialLdapContext(environment, null)

      // query the ldap server - note we replace the cn into the search filter
      val answer = context.search(searchRoot, searchFilter.replaceAll("\\{\\}", cn), searchControls)
      if (answer.hasMoreElements) {
        authString = Some(answer.next.getAttributes.get(authsAttribute).get().asInstanceOf[String])
      }
      answer.close
    } catch {
      case e: Exception => logger.error("Error querying ldap", e)
    } finally {
      try {
        if (context != null)
          context.close
      } catch {
        case e: Exception => logger.error("Error closing ldap connection", e)
      }
    }

    logger.debug("AuthorizationsProvider:: retrieved authorizations for user {} : {}", cn, authString)

    authString match {
      case None         => new Authorizations
      case Some(string) => new Authorizations(string.split(","): _*)
    }
  }
}

object GeoServerLdapAuthorizationsProvider {
  // name of the property file that will be used to configure this class
  val PROPS_FILE = "geomesa-ldap.properties"
  // properties names for configuring this provider -
  // the ldap node to start the query from
  val SEARCH_ROOT = "geomesa.ldap.search.root"
  // the query that will be applied to find the user's record
  val SEARCH_FILTER = "geomesa.ldap.search.filter"
  // the ldap attribute that holds the comma-delimited authorizations for the user
  val AUTHS_ATTRIBUTE = "geomesa.ldap.auths.attribute"
}