/*
 * Copyright (c) 2013-2015 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0 which
 * accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 */

package org.locationtech.geomesa.security

import java.io.Serializable
import java.util.{Map => jMap}

import scala.collection.JavaConversions._

object NoopAuditProvider extends AuditProvider {

  override def getCurrentUserId: String = "unknown"

  override def getCurrentUserDetails: jMap[AnyRef, AnyRef] = Map.empty[AnyRef, AnyRef]

  override def configure(params: jMap[String, Serializable]): Unit = {}
}
