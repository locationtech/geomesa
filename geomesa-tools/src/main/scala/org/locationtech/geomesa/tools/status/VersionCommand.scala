/***********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.tools.status

import com.beust.jcommander.Parameters
import org.locationtech.geomesa.tools.Command
import org.locationtech.geomesa.utils.conf.GeoMesaProperties

class VersionCommand extends Command {

  override val name = "version"
  override val params = new VersionParameters

  override def execute() = {
    import GeoMesaProperties._
    println(s"GeoMesa tools version: $ProjectVersion")
    println(s"Commit ID: $GitCommit")
    println(s"Branch: $GitBranch")
    println(s"Build date: $BuildDate")
  }
}

@Parameters(commandDescription = "Display the GeoMesa version installed locally")
class VersionParameters {}

