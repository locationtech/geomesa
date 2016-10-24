/***********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.tools.accumulo.commands

import com.beust.jcommander.{JCommander, Parameters}
import org.locationtech.geomesa.accumulo.data.AccumuloDataStore
import org.locationtech.geomesa.tools.accumulo.commands.AccumuloVersionCommand.AccumuloVersionParams
import org.locationtech.geomesa.tools.accumulo.{DataStoreParams, InstanceNameParams, OptionalCatalogParams, OptionalCredentialsParams}
import org.locationtech.geomesa.tools.common.commands.Command
import org.locationtech.geomesa.utils.conf.GeoMesaProperties._

import scala.util.control.NonFatal

class AccumuloVersionCommand(parent: JCommander) extends Command(parent) {

  override val command = "version"
  override val params = new AccumuloVersionParams

  override def execute() = {
    var ds: AccumuloDataStore = null
    val iterVersion = try {
      ds = params.createDataStore()
      ds.getVersion._2
    } catch {
      case NonFatal(e) => "unavailable (use optional arguments to check distributed runtime version)"
    } finally {
      if (ds != null) {
        ds.dispose()
      }
    }

    println(s"GeoMesa tools version: $ProjectVersion")
    println(s"Distributed runtime version: $iterVersion")
    println(s"Commit ID: $GitCommit")
    println(s"Branch: $GitBranch")
    println(s"Build date: $BuildDate")
  }
}

object AccumuloVersionCommand {

  @Parameters(commandDescription = "Display the installed GeoMesa version")
  class AccumuloVersionParams
      extends InstanceNameParams with OptionalCredentialsParams with OptionalCatalogParams with DataStoreParams {
    override val visibilities: String = null
    override val auths: String = null
  }
}
