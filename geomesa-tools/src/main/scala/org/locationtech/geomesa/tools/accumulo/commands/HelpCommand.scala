/***********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.tools.accumulo.commands

import com.beust.jcommander.JCommander
import org.locationtech.geomesa.tools.common.commands.AbstractHelpCommand
import org.locationtech.geomesa.tools.accumulo.AccumuloRunner.commandUsage

import scala.collection.JavaConversions._

class HelpCommand(parent: JCommander) extends AbstractHelpCommand(parent) {
  override def execute(): Unit =
    params.commandName.headOption match {
      case Some(command) => parent.usage(command)
      case None          =>
        println(commandUsage(parent) + "\nTo see help for a specific command type: geomesa help <command-name>\n")
    }
}

