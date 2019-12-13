/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.accumulo.security

import java.nio.charset.StandardCharsets

import org.apache.accumulo.core.security.Authorizations
import org.locationtech.geomesa.security.AuthorizationsProvider

import scala.collection.JavaConversions._

@deprecated
class AccumuloAuthsProvider(val authProvider: AuthorizationsProvider) {
  def getAuthorizations: Authorizations = {
    new Authorizations(authProvider.getAuthorizations.map(_.getBytes(StandardCharsets.UTF_8)))
  }
}
