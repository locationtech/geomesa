/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.utils.audit

import java.io.Serializable
import java.util.Collections

object NoOpAuditProvider extends AuditProvider {

  override val getCurrentUserId: String = "unknown"

  override val getCurrentUserDetails: java.util.Map[AnyRef, AnyRef] = Collections.emptyMap()

  override def configure(params: java.util.Map[String, Serializable]): Unit = {}
}
