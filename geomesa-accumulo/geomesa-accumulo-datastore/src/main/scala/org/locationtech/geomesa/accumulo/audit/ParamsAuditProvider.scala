/***********************************************************************
 * Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.accumulo.audit

import java.io.Serializable
import java.util.{Map => jMap}

import org.apache.accumulo.core.client.Connector
import org.locationtech.geomesa.accumulo.data.AccumuloDataStoreParams
import org.locationtech.geomesa.utils.audit.AuditProvider

import scala.collection.JavaConversions._

class ParamsAuditProvider extends AuditProvider {

  private var id = "unknown"

  override def getCurrentUserId: String = id

  override val getCurrentUserDetails: jMap[AnyRef, AnyRef] = Map.empty[AnyRef, AnyRef]

  override def configure(params: jMap[String, Serializable]): Unit = {
    import AccumuloDataStoreParams._
    val user = if (params.containsKey(connParam.key)) {
      connParam.lookUp(params).asInstanceOf[Connector].whoami()
    } else if (params.containsKey(userParam)) {
      userParam.lookUp(params).asInstanceOf[String]
    } else {
      null
    }
    if (user != null) {
      id = s"accumulo[$user]"
    }
  }

}
