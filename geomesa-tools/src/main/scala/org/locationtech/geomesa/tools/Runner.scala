/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.tools

import java.io.File

import com.beust.jcommander.{JCommander, ParameterException}
import com.typesafe.scalalogging.LazyLogging
import org.apache.commons.io.FileUtils
import org.locationtech.geomesa.tools.utils.GeoMesaIStringConverterFactory

import scala.collection.JavaConversions._
import scala.util.control.NonFatal

trait Runner extends LazyLogging {

  def name: String
  def environmentErrorInfo(): Option[String] = None

  def main(args: Array[String]): Unit = {
    try {
      parseCommand(args).execute()
    } catch {
      case e @ (_: ClassNotFoundException | _: NoClassDefFoundError) =>
        val msg = s"Warning: Missing dependency for command execution: ${e.getMessage}"
        logger.error(msg, e)
        Command.user.error(msg)
        environmentErrorInfo().foreach(Command.user.error)
        sys.exit(-1)
      case e: ParameterException => Command.user.error(e.getMessage); sys.exit(-1)
      case NonFatal(e) => Command.user.error(e.getMessage, e); sys.exit(-1)
    }
    sys.exit(0)
  }

  def parseCommand(args: Array[String]): Command = {
    val jc = new JCommander()
    jc.setProgramName(name)
    jc.addConverterFactory(new GeoMesaIStringConverterFactory)

    val commands = createCommands(jc)
    commands.foreach {
      case command: CommandWithSubCommands =>
        jc.addCommand(command.name, command.params)
        val registered = jc.getCommands.get(command.name)
        command.subCommands.foreach(sub => registered.addCommand(sub.name, sub.params))

      case command => jc.addCommand(command.name, command.params)
    }
    try {
      jc.parse(args: _*)
    } catch {
      case e: ParameterException =>
        Command.user.error(s"Error parsing arguments: ${e.getMessage}")
        Command.user.info(usage(jc, jc.getParsedCommand))
        throw e
    }
    val parsed = commands.find(_.name == jc.getParsedCommand).getOrElse(new DefaultCommand(jc))
    resolveEnvironment(parsed)
    parsed
  }

  def usage(jc: JCommander): String = {
    val out = new StringBuilder()
    out.append(s"Usage: $name [command] [command options]\n")
    val commands = jc.getCommands.map(_._1).toSeq
    out.append("  Commands:\n")
    val maxLen = commands.map(_.length).max + 4
    commands.sorted.foreach { command =>
      val spaces = " " * (maxLen - command.length)
      out.append(s"    $command$spaces${jc.getCommandDescription(command)}\n")
    }
    out.toString()
  }

  def usage(jc: JCommander, name: String): String = {
    Option(name).flatMap(n => Option(jc.getCommands.get(n))) match {
      case None => usage(jc)
      case Some(command) =>
        val out = new java.lang.StringBuilder()
        command.usage(out)
        out.toString
    }
  }

  def autocompleteUsage(jc: JCommander, autocompleteInfo: AutocompleteInfo): Unit = {
    val file = new File(autocompleteInfo.path)
    val commands = jc.getCommands.map(_._1).toSeq
    val out = new StringBuilder
    out.append(
      s"""_${autocompleteInfo.commandName}(){
         |  local cur prev;
         |  COMPREPLY=();
         |  cur="$${COMP_WORDS[COMP_CWORD]}";
         |  prev="$${COMP_WORDS[COMP_CWORD-1]}";
         |
         |  if [[ "$${COMP_WORDS[1]}" == "help" ]]; then
         |    COMPREPLY=( $$(compgen -W "${commands.mkString(" ")}" $${cur}));
         |    return 0;
         |  fi;
         |
         |  case $${COMP_CWORD} in
         |    1)
         |      COMPREPLY=( $$(compgen -W "${commands.mkString(" ")}" $${cur}));
         |      ;;
         |    [2-9] | [1-9][0-9])
         |      if [[ "$${cur}" =~ ^-[a-zA-Z-]?+$$ ]]; then
         |        case $${COMP_WORDS[1]} in
        """.stripMargin)
    commands.foreach { command =>
      val params = jc.getCommands.get(command).getParameters.filter(!_.getParameter.hidden()).flatMap(_.getParameter.names().filter(_.length != 2))
        out.append(
      s"""            $command)
         |              COMPREPLY=( $$(compgen -W "${params.mkString(" ").replaceAll("[,\\s]+", " ")}" -- $${cur}));
         |              return 0;
         |              ;;
      """.stripMargin)
    }
    out.append(
      s"""        esac;
         |      else
         |        compopt -o filenames -o nospace;
         |        COMPREPLY=( $$(compgen -f "$$2") );
         |      fi;
         |      return 0;
         |      ;;
         |    *)
         |      COMPREPLY=();
         |      ;;
         |  esac;
         |};
         |complete -F _${autocompleteInfo.commandName} ${autocompleteInfo.commandName};
         |complete -F _${autocompleteInfo.commandName} bin/${autocompleteInfo.commandName};
         |
         |
       """.stripMargin)
    FileUtils.writeStringToFile(file, out.toString())
  }

  protected def createCommands(jc: JCommander): Seq[Command]
  protected def resolveEnvironment(command: Command): Unit = {}

  class DefaultCommand(jc: JCommander) extends Command {
    override def execute(): Unit = Command.user.info(usage(jc))
    override val name: String = ""
    override val params: Any = null
  }
}

case class AutocompleteInfo(path: String, commandName: String)
