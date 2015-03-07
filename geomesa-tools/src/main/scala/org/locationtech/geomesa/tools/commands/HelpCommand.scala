/*
 * Copyright 2014 Commonwealth Computer Research, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the License);
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an AS IS BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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
