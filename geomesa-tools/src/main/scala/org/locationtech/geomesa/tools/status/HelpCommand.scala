/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.tools.status

import com.beust.jcommander.{JCommander, Parameter, Parameters}
import org.locationtech.geomesa.tools.{AutocompleteInfo, Command, Runner}

class HelpCommand(runner: Runner, jc: JCommander) extends Command {

  override val name: String = "help"
  override val params = new HelpParameters

  override def execute(): Unit = {
    if (params.autocompleteInfo != null) {
      val autocompleteInfo = AutocompleteInfo(params.autocompleteInfo.get(0), params.autocompleteInfo.get(1))
      runner.autocompleteUsage(jc, autocompleteInfo)
      Command.output.info(s"Wrote Autocomplete function to ${autocompleteInfo.path}.")
    } else if (params.command == null || params.command.isEmpty) {
      Command.output.info(s"${runner.usage(jc)}\nTo see help for a specific command type: ${runner.name} help <command-name>\n")
    } else {
      Command.output.info(runner.usage(jc, params.command.get(0)))
    }
  }
}

@Parameters(commandDescription = "Show help")
class HelpParameters {
  @Parameter(description = "Help for a specific command", required = false)
  val command: java.util.List[String] = null

  @Parameter(names = Array("--autocomplete-function"), description = "Generates and outputs a bash function for " +
    "autocompleting GeoMesa commandline commands and their parameters. First value is output path, second is command name",
    required = false, hidden = true)
  val autocompleteInfo: java.util.List[String] = null
}
