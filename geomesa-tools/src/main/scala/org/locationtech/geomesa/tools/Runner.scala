/***********************************************************************
 * Copyright (c) 2013-2021 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.tools

import java.io.File
import java.nio.charset.StandardCharsets
import java.util.concurrent.atomic.{AtomicInteger, AtomicLong}

import com.beust.jcommander.{JCommander, ParameterException}
import com.codahale.metrics.{MetricRegistry, Timer}
import com.github.benmanes.caffeine.cache.{CacheLoader, Caffeine, LoadingCache}
import com.typesafe.scalalogging.LazyLogging
import org.apache.commons.io.FileUtils
import org.locationtech.geomesa.tools.Command.CommandException
import org.locationtech.geomesa.tools.Runner.AutocompleteInfo
import org.locationtech.geomesa.tools.`export`.{ConvertCommand, GenerateAvroSchemaCommand}
import org.locationtech.geomesa.tools.help.{ClasspathCommand, HelpCommand, NailgunCommand, ScalaConsoleCommand}
import org.locationtech.geomesa.tools.status.{ConfigureCommand, EnvironmentCommand, VersionCommand}
import org.locationtech.geomesa.tools.utils.{GeoMesaIStringConverterFactory, TerminalCallback}
import org.locationtech.geomesa.utils.stats.MethodProfiling

import scala.collection.JavaConversions._
import scala.util.control.NonFatal

trait Runner extends MethodProfiling with LazyLogging {

  def name: String
  def environmentErrorInfo(): Option[String] = None

  def main(args: Array[String]): Unit = {
    try {
      Runner.execute(parseCommand(args))
    } catch {
      case e @ (_: ClassNotFoundException | _: NoClassDefFoundError) =>
        val msg = s"Warning: Missing dependency for command execution: ${e.getMessage}"
        logger.error(msg, e)
        Command.user.error(msg)
        environmentErrorInfo().foreach(Command.user.error)
        sys.exit(1)
      case e: ParameterException => Command.user.error(e.getMessage); sys.exit(1)
      case e: CommandException => Command.user.error(e.getMessage); sys.exit(1)
      case NonFatal(e) => Command.user.error(e.getMessage, e); sys.exit(1)
    }
    sys.exit(0)
  }

  def parseCommand(args: Array[String]): Command = {
    val jc =
      JCommander.newBuilder()
          .programName(name)
          .addConverterFactory(new GeoMesaIStringConverterFactory)
          .columnSize(math.max(79, TerminalCallback.terminalWidth().toInt))
          .build()

    val commands = this.commands :+ new HelpCommand(this, jc)
    commands.foreach { command =>
      jc.addCommand(command.name, command.params)
      command.subCommands.foreach(sub => jc.getCommands.get(command.name).addCommand(sub.name, sub.params))
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
    if (parsed.subCommands.isEmpty) { parsed } else {
      lazy val available =
        s"Use '${parsed.name} <sub-command>', where sub-command is one of: " +
            parsed.subCommands.map(_.name).mkString(", ")
      val sub = Option(jc.getCommands.get(parsed.name).getParsedCommand).getOrElse {
        throw new ParameterException(s"No sub-command specified. $available")
      }
      parsed.subCommands.find(_.name == sub).getOrElse {
        throw new ParameterException(s"Sub-command '$sub' not found. $available")
      }
    }
  }

  def usage(jc: JCommander): String = {
    val out = new StringBuilder()
    out.append(s"Usage: $name [command] [command options]\n")
    val commands = jc.getCommands.map(_._1).toSeq.sorted
    out.append("  Commands:\n")
    val maxLen = commands.map(_.length).max + 4
    commands.foreach { name =>
      val spaces = " " * (maxLen - name.length)
      out.append(s"    $name$spaces${jc.getUsageFormatter.getCommandDescription(name)}\n")
    }
    out.toString()
  }

  def usage(jc: JCommander, name: String): String = {
    Option(name).flatMap(n => Option(jc.getCommands.get(n))) match {
      case None => usage(jc)
      case Some(command) =>
        val out = new java.lang.StringBuilder()
        command.getUsageFormatter.usage(out)
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
    FileUtils.writeStringToFile(file, out.toString(), StandardCharsets.UTF_8)
  }

  /**
   * Commands available to this runner. The default impl handles common commands
   * and placeholders for script functions
   *
   * @return
   */
  protected def commands: Seq[Command] = {
    Seq(
      new ConvertCommand,
      new ConfigureCommand,
      new ClasspathCommand,
      new EnvironmentCommand,
      new GenerateAvroSchemaCommand,
      new NailgunCommand,
      new ScalaConsoleCommand,
      new VersionCommand
    )
  }

  protected def resolveEnvironment(command: Command): Unit = {}

  class DefaultCommand(jc: JCommander) extends Command {
    override def execute(): Unit = Command.user.info(usage(jc))
    override val name: String = ""
    override val params: Any = null
  }
}

object Runner {

  private val registry = new MetricRegistry()

  val FirstRequest: Long = System.currentTimeMillis()
  val LastRequest = new AtomicLong(System.currentTimeMillis())

  val Timers: LoadingCache[String, (AtomicInteger, Timer)] = Caffeine.newBuilder().build(
    new CacheLoader[String, (AtomicInteger, Timer)]() {
      override def load(k: String): (AtomicInteger, Timer) = (new AtomicInteger(0), registry.timer(k))
    }
  )

  def execute(command: Command): Unit = {
    LastRequest.set(System.currentTimeMillis())
    val (active, timer) = Timers.get(command.name)
    active.incrementAndGet()
    try { timer.time(command) } finally {
      active.decrementAndGet()
    }
  }

  case class AutocompleteInfo(path: String, commandName: String)
}
