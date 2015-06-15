/***********************************************************************
* Copyright (c) 2013-2015 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0 which
* accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/
package org.locationtech.geomesa.tools.commands

import java.util.Properties

import com.beust.jcommander.{JCommander, Parameters}
import org.locationtech.geomesa.tools.commands.VersionCommand._

class VersionCommand(parent: JCommander) extends Command(parent) {

  override val command = "version"
  override val params = new VersionParameters

  override def execute() = {
    val properties = new Properties()
    val stream = getClass
                  .getClassLoader
                  .getResourceAsStream(propertiesPath)

    properties.load(stream)
    println(s"GeoMesa Version ${properties.getProperty("geomesa.build.version")} " +
            s"built on ${properties.getProperty("geomesa.build.date")}.")
    println(s"Commit ID: ${properties.getProperty("geomesa.build.commit.id")}")
    println(s"Branch: ${properties.getProperty("geomesa.build.branch")}")
    stream.close()
  }
}

object VersionCommand {
  val propertiesPath ="org/locationtech/geomesa/tools/geomesaVersion.properties"
  @Parameters(commandDescription = "GeoMesa Version")
  class VersionParameters {}
}
