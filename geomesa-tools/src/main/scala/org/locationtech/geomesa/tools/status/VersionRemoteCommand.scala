/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.tools.status

import org.locationtech.geomesa.index.geotools.GeoMesaDataStore
import org.locationtech.geomesa.tools._
import org.locationtech.geomesa.utils.conf.GeoMesaProperties._

import scala.util.control.NonFatal
import org.locationtech.geomesa.index.conf.SchemaProperties

trait VersionRemoteCommand[DS <: GeoMesaDataStore[_]] extends DataStoreCommand[DS] {

  override val name: String = "version-remote"

  override def execute(): Unit = {
    Command.output.info(s"Local GeoMesa tools version: $ProjectVersion")
    Command.output.info(s"Local Commit ID: $GitCommit")
    Command.output.info(s"Local Branch: $GitBranch")
    Command.output.info(s"Local Build date: $BuildDate")

    val check = System.getProperty(SchemaProperties.CheckDistributedVersion.property)
    val validate = System.getProperty(SchemaProperties.ValidateDistributedClasspath.property)
    try {
      SchemaProperties.CheckDistributedVersion.set("true")
      SchemaProperties.ValidateDistributedClasspath.set("false")
      val iterVersion = withDataStore(_.getDistributedVersion.map(_.toString))
      Command.output.info(s"Distributed runtime version: ${iterVersion.getOrElse("unknown")}")
    } catch {
      case NonFatal(e) => Command.user.error("Could not get distributed version:", e)
    } finally {
      if (check == null) {
        SchemaProperties.CheckDistributedVersion.clear()
      } else {
        SchemaProperties.CheckDistributedVersion.set(check)
      }
      if (validate == null) {
        SchemaProperties.ValidateDistributedClasspath.clear()
      } else {
        SchemaProperties.ValidateDistributedClasspath.set(validate)
      }
    }
  }
}
