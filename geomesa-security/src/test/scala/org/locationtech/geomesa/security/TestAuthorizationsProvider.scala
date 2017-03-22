/***********************************************************************
 * Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.security

import org.apache.accumulo.core.security.Authorizations
import org.springframework.security.authentication.TestingAuthenticationToken
import org.springframework.security.core.context.SecurityContextHolder

class TestAuthorizationsProvider extends AuthorizationsProvider {
  override def getAuthorizations: Authorizations = {
    import scala.collection.JavaConversions._
    val authentication = SecurityContextHolder.getContext.getAuthentication.asInstanceOf[TestingAuthenticationToken]
    val authorities = authentication.getAuthorities.map(_.getAuthority).toSeq
    new Authorizations(authorities: _*)
  }

  override def configure(params: java.util.Map[String, java.io.Serializable]): Unit = {}

}
