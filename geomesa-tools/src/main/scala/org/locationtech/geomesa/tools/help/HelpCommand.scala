/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.tools.help

import com.beust.jcommander.{JCommander, Parameter, Parameters}
import org.locationtech.geomesa.tools.help.HelpCommand.HelpParameters
import org.locationtech.geomesa.tools.status.AutoCompleteCommand
import org.locationtech.geomesa.tools.{Command, Runner}

import java.io.File

class HelpCommand(runner: Runner, jc: JCommander) extends Command {

  override val name: String = "help"
  override val params = new HelpParameters

  // noinspection ScalaDeprecation
  override def execute(): Unit = {
    if (params.autocompleteInfo != null) {
      val autocomplete = new AutoCompleteCommand(runner, jc)
      autocomplete.params.file = new File(params.autocompleteInfo.get(0))
      autocomplete.execute()
    } else if (params.command == null || params.command.isEmpty) {
      Command.output.info(s"${runner.usage(jc)}\nTo see help for a specific command type: ${runner.name} help <command-name>\n")
    } else {
      Command.output.info(runner.usage(jc, params.command.get(0)))
    }
  }
}

object HelpCommand {
  @Parameters(commandDescription = "Show help")
  class HelpParameters {
    @Parameter(description = "Help for a specific command", required = false)
    var command: java.util.List[String] = _

    @deprecated
    @Parameter(names = Array("--autocomplete-function"), required = false, hidden = true)
    var autocompleteInfo: java.util.List[String] = _
  }
}
