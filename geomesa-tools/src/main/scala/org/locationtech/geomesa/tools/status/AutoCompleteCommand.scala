/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.tools.status

import com.beust.jcommander.{JCommander, Parameter, Parameters}
import org.locationtech.geomesa.tools.status.AutoCompleteCommand.AutoCompleteParameters
import org.locationtech.geomesa.tools.utils.Prompt
import org.locationtech.geomesa.tools.{Command, OptionalForceParam, Runner}
import org.locationtech.geomesa.utils.io.WithClose

import java.io.{File, FileWriter}
import java.nio.charset.StandardCharsets
import scala.io.Source

/**
 * Autocomplete command
 */
class AutoCompleteCommand(runner: Runner, jc: JCommander) extends Command {

  import scala.collection.JavaConverters._

  override val name = "autocomplete"
  override val params = new AutoCompleteParameters()

  override def execute(): Unit = {
    if (params.force ||
        Prompt.confirm(s"Register auto-complete for ${runner.name} commands to ${params.file.getAbsolutePath} (y/n)? ", default = "y")) {
      val exists = params.file.exists() &&
        WithClose(Source.fromFile(params.file)(StandardCharsets.UTF_8)) { source =>
          source.getLines().exists(_.startsWith(s"_${runner.name}()"))
        }
      if (exists) {
        Command.user.info("Auto-complete function already installed")
      } else {
        if (params.file.getParentFile != null && !params.file.getParentFile.exists()) {
          params.file.getParentFile.mkdirs()
        }
        WithClose(new FileWriter(params.file, StandardCharsets.UTF_8, true)) { writer =>
          val commands = jc.getCommands.asScala.keys.toSeq.sorted
          writer.write(
            s"""# begin added by ${runner.name}
               |_${runner.name}(){
               |  local cur prev;
               |  COMPREPLY=();
               |  cur="$${COMP_WORDS[COMP_CWORD]}";
               |  prev="$${COMP_WORDS[COMP_CWORD-1]}";
               |
               |  case $${COMP_CWORD} in
               |    1)
               |      COMPREPLY=( $$(compgen -W "${commands.mkString(" ")}" $${cur}));
               |      ;;
               |    *)
               |      case $${COMP_WORDS[1]} in
               |""".stripMargin)
          commands.foreach { command =>
            writer.write(
              s"          $command)\n")
            if (command == "help") {
              writer.write(
            s"""            COMPREPLY=( $$(compgen -W "${commands.mkString(" ")}" $${cur}));
               |""".stripMargin)
            } else {
              val params = jc.getCommands.get(command).getParameters.asScala.filterNot(_.getParameter.hidden())
              val flags = params.flatMap(_.getParameter.names().filterNot(_.length == 2)/* filter out short flag variants */)
              // note: this isn't universally true, but it is for all our commands
              val hasFiles = jc.getCommands.get(command).getMainParameter != null
              // TODO we could compare `prev` and check the arity of the command to know whether an arg is expected
              // TODO we could also do some kind of check or override to determine if the arg is a file or not
              if (hasFiles) {
          writer.write(
            s"""            if [[ "$${cur}" =~ ^-[a-zA-Z-]?+$$ ]]; then
               |              COMPREPLY=( $$(compgen -W "${flags.mkString(" ").replaceAll("[,\\s]+", " ")}" -- $${cur}));
               |            else
               |              compopt -o filenames -o nospace;
               |              COMPREPLY=( $$(compgen -f "$$2") );
               |            fi
               |""".stripMargin)
              } else {
          writer.write(
            s"""            if [[ -z "$${cur}" ]] || [[ "$${cur}" =~ ^-[a-zA-Z-]?+$$ ]]; then
               |              COMPREPLY=( $$(compgen -W "${flags.mkString(" ").replaceAll("[,\\s]+", " ")}" -- $${cur}));
               |            else
               |              compopt -o filenames -o nospace;
               |              COMPREPLY=( $$(compgen -f "$$2") );
               |            fi
               |""".stripMargin)
              }
            }
            writer.write(
            s"""            return 0;
               |            ;;
               |
               |""".stripMargin)
          }
          writer.write(
            s"""      esac;
               |      ;;
               |  esac;
               |};
               |complete -F _${runner.name} ${runner.name};
               |complete -F _${runner.name} bin/${runner.name};
               |# end added by ${runner.name}
               |""".stripMargin)
        }
        Command.user.info("Auto-complete installed, to use now run:")
        Command.user.info(s"  source ${params.file.getAbsolutePath}")
      }
    }
  }
}

object AutoCompleteCommand {
  @Parameters(commandDescription = "Configure local autocomplete for GeoMesa functions")
  class AutoCompleteParameters extends OptionalForceParam {
    @Parameter(names = Array("-o", "--output"), description = "Location to output autocomplete scripts")
    var file: File = new File(s"${sys.env("HOME")}/.bash_completion")
  }
}
