/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.tools.status

import com.beust.jcommander.Parameters
import org.locationtech.geomesa.tools.Command
import org.locationtech.geomesa.utils.conf.GeoMesaProperties

class VersionCommand extends Command {

  override val name = "version"
  override val params = new VersionParameters
  override def execute(): Unit = {
    import GeoMesaProperties._
    Command.output.info(s"GeoMesa tools version: $ProjectVersion")
    Command.output.info(s"Commit ID: $GitCommit")
    Command.output.info(s"Branch: $GitBranch")
    Command.output.info(s"Build date: $BuildDate")
  }
}

@Parameters(commandDescription = "Display the GeoMesa version installed locally")
class VersionParameters {}

