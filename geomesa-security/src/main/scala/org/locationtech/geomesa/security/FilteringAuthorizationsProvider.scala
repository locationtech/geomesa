/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.security

import java.util

import scala.collection.JavaConversions._

/**
 * AuthorizationsProvider that wraps another provider and ensures that the auths returned do not exceed a pre-set list
 */
class FilteringAuthorizationsProvider (val wrappedProvider: AuthorizationsProvider)
    extends AuthorizationsProvider {

  private var filter: Option[Array[String]] = None

  override def getAuthorizations: util.List[String] =
    filter match {
      case None    => wrappedProvider.getAuthorizations
      case Some(f) => wrappedProvider.getAuthorizations.intersect(f)
    }

  override def configure(params: java.util.Map[String, java.io.Serializable]): Unit = {
    filter = AuthsParam.lookupOpt(params).filterNot(_.isEmpty).map(_.split(","))
    wrappedProvider.configure(params)
  }
}
