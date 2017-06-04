/***********************************************************************
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.accumulo.security

import org.locationtech.geomesa.security.AuthorizationsProvider
import org.springframework.security.authentication.TestingAuthenticationToken
import org.springframework.security.core.context.SecurityContextHolder

class TestAuthorizationsProvider extends AuthorizationsProvider {
  override def getAuthorizations: java.util.List[String] = {
    import scala.collection.JavaConversions._
    val authentication = SecurityContextHolder.getContext.getAuthentication.asInstanceOf[TestingAuthenticationToken]
    new java.util.ArrayList[String](authentication.getAuthorities.map(_.getAuthority))
  }

  override def configure(params: java.util.Map[String, java.io.Serializable]): Unit = {}

}
