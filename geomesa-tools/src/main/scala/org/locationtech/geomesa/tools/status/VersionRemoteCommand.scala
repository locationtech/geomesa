/***********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.tools.status

import org.locationtech.geomesa.index.geotools.GeoMesaDataStore
import org.locationtech.geomesa.tools._
import org.locationtech.geomesa.utils.conf.GeoMesaProperties._

import scala.util.control.NonFatal

trait VersionRemoteCommand[DS <: GeoMesaDataStore[_, _, _]] extends DataStoreCommand[DS] {

  override val name: String = "version-remote"

  override def execute(): Unit = {
    println(s"GeoMesa tools version: $ProjectVersion")
    try {
      val iterVersion = withDataStore(_.getVersion._2)
      println(s"Distributed runtime version: $iterVersion")
    } catch {
      case NonFatal(e) => logger.error("Could not get distributed version:", e)
    }
    println(s"Commit ID: $GitCommit")
    println(s"Branch: $GitBranch")
    println(s"Build date: $BuildDate")
  }
}
