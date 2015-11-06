/***********************************************************************
* Copyright (c) 2013-2015 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0 which
* accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/
package org.locationtech.geomesa.tools.commands

import com.beust.jcommander.{JCommander, Parameters}
import com.typesafe.scalalogging.slf4j.Logging
import org.locationtech.geomesa.tools.FeatureCreator
import org.locationtech.geomesa.tools.commands.CreateCommand.CreateParameters

class CreateCommand(parent: JCommander) extends Command(parent) with Logging {
  override val command = "create"
  override val params = new CreateParameters()
  override def execute() = FeatureCreator.createFeature(params)
}

object CreateCommand {
  @Parameters(commandDescription = "Create a feature definition in a GeoMesa catalog")
  class CreateParameters extends CreateFeatureParams {}
}