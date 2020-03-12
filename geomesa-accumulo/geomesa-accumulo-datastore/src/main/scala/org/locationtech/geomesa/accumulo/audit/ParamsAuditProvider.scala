/***********************************************************************
 * Copyright (c) 2013-2020 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.accumulo.audit

import java.io.Serializable
import java.util.Collections

import org.locationtech.geomesa.accumulo.data.AccumuloDataStoreParams
import org.locationtech.geomesa.utils.audit.AuditProvider

class ParamsAuditProvider extends AuditProvider {

  private var id = "unknown"

  override def getCurrentUserId: String = id

  override val getCurrentUserDetails: java.util.Map[AnyRef, AnyRef] = Collections.emptyMap()

  override def configure(params: java.util.Map[String, _ <: Serializable]): Unit = {
    id = AccumuloDataStoreParams.UserParam.lookupOpt(params).map(u => s"accumulo[$u]").getOrElse("unknown")
  }
}
