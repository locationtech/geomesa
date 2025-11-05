/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.tools.status

import com.beust.jcommander.Parameters
import org.locationtech.geomesa.tools.Command
import org.locationtech.geomesa.tools.status.ConfigureCommand.ConfigureParameters

/**
  * Note: this class is a placeholder for the 'configure' function implemented in the 'geomesa-*' script, to get it
  * to show up in the JCommander help
  */
class ConfigureCommand extends Command {
  override val name = "configure"
  override val params = new ConfigureParameters
  override def execute(): Unit = {}
}

object ConfigureCommand {
  @Parameters(commandDescription = "Configure the local environment for GeoMesa")
  class ConfigureParameters {}
}
