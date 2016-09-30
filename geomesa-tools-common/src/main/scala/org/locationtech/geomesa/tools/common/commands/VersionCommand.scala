/***********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.tools.common.commands

import com.beust.jcommander.{JCommander, Parameters}
import org.locationtech.geomesa.tools.common.commands.VersionCommand._
import org.locationtech.geomesa.utils.conf.GeoMesaProperties

class VersionCommand(parent: JCommander) extends Command(parent) {

  override val command = "version"
  override val params = new VersionParameters

  override def execute() = {
    import GeoMesaProperties._
    println(s"GeoMesa version $ProjectVersion built on $BuildDate.")
    println(s"Commit ID: $GitCommit")
    println(s"Branch: $GitBranch")
  }
}

object VersionCommand {
  @Parameters(commandDescription = "Display the installed GeoMesa version")
  class VersionParameters {}
}
