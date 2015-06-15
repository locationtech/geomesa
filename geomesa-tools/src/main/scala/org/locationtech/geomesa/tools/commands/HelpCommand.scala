/***********************************************************************
* Copyright (c) 2013-2015 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0 which
* accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/
package org.locationtech.geomesa.tools.commands

import java.util

import com.beust.jcommander.{JCommander, Parameter, Parameters}
import org.locationtech.geomesa.tools.Runner.commandUsage
import org.locationtech.geomesa.tools.commands.HelpCommand.HelpParameters

import scala.collection.JavaConversions._

class HelpCommand(parent: JCommander) extends Command(parent) {
  override val command = "help"
  override val params = new HelpParameters

  override def execute(): Unit =
    params.commandName.headOption match {
      case Some(command) => parent.usage(command)
      case None          =>
        println(commandUsage(parent) + "\nTo see help for a specific command type: geomesa help <command-name>\n")
    }

}

object HelpCommand {
  @Parameters(commandDescription = "Show help")
  class HelpParameters {
    @Parameter(description = "commandName", required = false)
    val commandName: util.List[String] = new util.ArrayList[String]()
  }

}
