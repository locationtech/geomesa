/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.security

/**
 * Default implementation of AuthorizationsProvider that provides a static set of authorizations,
 * generally loaded from config
 */
class DefaultAuthorizationsProvider(authorizations: Seq[String]) extends AuthorizationsProvider {

  import scala.collection.JavaConverters._

  private val asJava = authorizations.asJava

  override def getAuthorizations: java.util.List[String] = asJava
  override def configure(params: java.util.Map[String, _]): Unit = {}
}
