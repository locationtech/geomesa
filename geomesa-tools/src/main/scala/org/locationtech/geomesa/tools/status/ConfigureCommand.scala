/***********************************************************************
 * Copyright (c) 2013-2021 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.tools.status

import com.beust.jcommander.Parameters
import org.locationtech.geomesa.tools.Command

/**
  * Note: this class is a placeholder for the 'configure' function implemented in the 'geomesa-*' script, to get it
  * to show up in the JCommander help
  */
class ConfigureCommand extends Command {

  override val name = "configure"
  override val params = new ConfigureParameters
  override def execute(): Unit = {}
}

@Parameters(commandDescription = "Configure the local environment for GeoMesa")
class ConfigureParameters {}
