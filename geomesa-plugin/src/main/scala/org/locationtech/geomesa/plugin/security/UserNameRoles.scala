/*
 * Copyright 2014 Commonwealth Computer Research, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.locationtech.geomesa.plugin.security

import org.geoserver.security.GeoServerRoleService
import org.geoserver.security.config.SecurityNamedServiceConfig
import org.geoserver.security.impl.GeoServerRole
import org.geoserver.security.xml.{XMLSecurityProvider, XMLRoleService}

import java.util.{SortedSet => jSortedSet, TreeSet => jTreeSet, Map => jMap, HashMap => jHashMap, Collections}

import scala.collection.mutable
import scala.collection.JavaConversions._

object UserNameRoles {
  private val isUserRoleProp = "IS_USER_ROLE"
  private def isUserRole(role: GeoServerRole) = role.getProperties.getProperty("IS_USER_ROLE") == "true"
  private def userRole(username: String) = {
    val r = new GeoServerRole(username)
    r.setUserName(username)
    r.getProperties.setProperty(isUserRoleProp, "true")
    r
  }
}

import UserNameRoles.{userRole, isUserRole}

trait UserNameRoles extends GeoServerRoleService {
  private[this] val storedUserRoles: mutable.HashMap[String, GeoServerRole] =
    new mutable.HashMap[String, GeoServerRole] {
      override def default(username: String) = {
        val role = userRole(username)
        update(username, role)
        role
      }
    }

  // this is the core of what we're trying to do:
  // the user's roles ALSO include a new role solely for their use.
  // this must be created on the fly with the user, and since standard (XML)
  // RoleServices don't write back to their backing configuration
  // we need everything to happen as a layer on top of the configured answers
  abstract override def getRolesForUser(username: String): jSortedSet[GeoServerRole] = {
    val roles = new jTreeSet[GeoServerRole]()
    roles.addAll(super.getRolesForUser(username))
    roles.add(storedUserRoles(username))
    Collections.unmodifiableSortedSet(roles)
  }

  // the rest of this is to try to provide consistency with the above piece

  // the only group for a UserRole is the user's identity group
  abstract override def getGroupNamesForRole(role: GeoServerRole): jSortedSet[String] = {
    if (isUserRole(role)) {
      val groupNames = new jTreeSet[String]()
      groupNames.add(role.getUserName)
      Collections.unmodifiableSortedSet(groupNames)
    } else super.getGroupNamesForRole(role)
  }

  // the only user name in a UserRole is the user itself
  abstract override def getUserNamesForRole(role: GeoServerRole): jSortedSet[String] = {
    if (isUserRole(role)) {
      val userNames = new jTreeSet[String]()
      userNames.add(role.getUserName)
      Collections.unmodifiableSortedSet(userNames)
    } else super.getUserNamesForRole(role)
  }

  abstract override def getParentMappings: jMap[String, String] = {
    val mappings = new jHashMap[String, String]
    mappings.putAll(super.getParentMappings)
    for ((username, _) <- storedUserRoles) mappings.put(username, null)
    Collections.unmodifiableMap(mappings)
  }

  // if we're accessing by group it's almost certainly NOT a user's identity group (I hope)
  // thus getRolesForGroup(groupname: String): jSortedSet[GeoServerRole] stays the same

  // can't see how to automagically access the bean-provided UserService, so can't list all
  // user name roles in the call to getRoles(). However, we CAN list the roles we've seen before...
  abstract override def getRoles: jSortedSet[GeoServerRole] = {
    val roles = new jTreeSet[GeoServerRole]
    roles.addAll(super.getRoles)
    for (ugService <- getSecurityManager.loadUserGroupServices; user <- ugService.getUsers) {
      val username = user.getUsername
      val userrole = storedUserRoles(username)
      roles.add(userrole)
    }
    Collections.unmodifiableSortedSet(roles)
  }
}

class XMLUserNameRoleService extends XMLRoleService with UserNameRoles

// The RoleService seems to be deployed from the SecurityProvider, so this will replace
// that with one that creates an XMLUserNameRoleService instead of an XMLRoleService
// In turn, the SecurityProvider is deployed as a bean in the gs-main JAR, specified in
// applicationSecurityContext.xml.  To replace it
// 1) unzip the gs-main JAR into a spare directory
// 2) edit applicationSecurityContext.xml to replace XMLSecurityProvider with
//    XMLUserNameRoleSecurityProvider (including package name!)
// 3) bundle the contents of the directory into a new JAR with "jar cf"
// 4) replace the gs-main JAR from geoserver with the new JAR
// 5) edit $GEOSERVER_DATA/security/role/default/config.xml to replace XMLRoleService
//    with XMLUserNameRoleService (including package name!)
// then when GeoServer deploys it should create this SecurityProvider instead
class XMLUserNameRoleSecurityProvider extends XMLSecurityProvider {
  override def getRoleServiceClass: Class[_ <: GeoServerRoleService] = classOf[XMLUserNameRoleService]

  override def createRoleService(config: SecurityNamedServiceConfig): GeoServerRoleService =
    new XMLUserNameRoleService()
}