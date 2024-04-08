/***********************************************************************
 * Copyright (c) 2013-2024 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.tools

import com.beust.jcommander.{JCommander, ParameterException}
import com.facebook.nailgun.NGContext
import com.typesafe.scalalogging.LazyLogging
import org.apache.commons.io.FileUtils
import org.locationtech.geomesa.tools.Command.CommandException
import org.locationtech.geomesa.tools.Runner.{AutocompleteInfo, CommandResult, Executor}
import org.locationtech.geomesa.tools.`export`.{ConvertCommand, GenerateAvroSchemaCommand}
import org.locationtech.geomesa.tools.help.{ClasspathCommand, HelpCommand, NailgunCommand, ScalaConsoleCommand}
import org.locationtech.geomesa.tools.status.{ConfigureCommand, EnvironmentCommand, VersionCommand}
import org.locationtech.geomesa.tools.utils.{GeoMesaIStringConverterFactory, NailgunServer}
import org.locationtech.geomesa.utils.stats.MethodProfiling

import java.io.File
import java.nio.charset.StandardCharsets
import java.util.Locale
import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer
import scala.util.control.NonFatal

trait Runner extends MethodProfiling with LazyLogging {

  def name: String

  /**
   * A list of environment variables used to load the classpath, used for error messages after
   * ClassNotFoundExceptions
   *
   * @return
   */
  protected def classpathEnvironments: Seq[String] = Seq.empty

  def main(args: Array[String]): Unit = execute(new MainExecutor(args))

  def nailMain(context: NGContext): Unit = execute(new NailgunExecutor(context))

  private def execute(executor: Executor): Unit = {
    val result = try { executor.execute(); CommandResult(0) } catch {
      case e @ (_: ClassNotFoundException | _: NoClassDefFoundError) =>
        // log the underling exception to the log file, but don't show to the user
        val msg = s"Warning: Missing dependency for command execution: ${e.getMessage}"
        logger.error(msg, e)
        CommandResult(1, Seq(Left(msg)) ++ getEnvironmentErrors.map(Left.apply[String, Throwable]))
      case e @ (_: ParameterException | _: CommandException) =>
        logger.error("Error executing command:", e)
        CommandResult(1, failureMessages(e))
      case NonFatal(e) =>
        logger.error("Error executing command:", e)
        CommandResult(1, Seq(Right(e)))
    }

    result.errors.foreach {
      case Left(msg) => Command.user.error(msg)
      case Right(e)  => Command.user.error(e.getMessage, e)
    }

    sys.exit(result.code)
  }

  private def failureMessages(e: Throwable, indent: String = "", prefix: String = ""): Seq[Left[String, Throwable]] = {
    val messages = ArrayBuffer.empty[Left[String, Throwable]]
    val message = if (indent == "") { e.getMessage } else { s"$indent$prefix${e.toString}" }
    messages += Left(message)
    if (e.getCause != null) {
      messages ++= failureMessages(e.getCause, indent + "  ", "Caused by: ")
    }
    e.getSuppressed.foreach { e =>
      messages ++= failureMessages(e, indent + "  ", "Suppressed: ")
    }
    messages.toSeq
  }

  def parseCommand(args: Array[String]): Command = {
    val jc =
      JCommander.newBuilder()
          .programName(name)
          .addConverterFactory(new GeoMesaIStringConverterFactory)
          .build()

    val commands = this.commands :+ new HelpCommand(this, jc)
    commands.foreach { command =>
      jc.addCommand(command.name, command.params)
      command.subCommands.foreach(sub => jc.getCommands.get(command.name).addCommand(sub.name, sub.params))
    }

    def logAndThrowError(e: ParameterException): Unit = {
      Command.user.error(s"Error parsing arguments: ${e.getMessage}")
      Command.user.info(usage(jc, jc.getParsedCommand))
      throw e
    }

    try { jc.parse(args: _*) } catch {
      case e: ParameterException => logAndThrowError(e)
    }

    val parsed = commands.find(_.name == jc.getParsedCommand).getOrElse(new DefaultCommand(jc))
    resolveEnvironment(parsed)
    val command = if (parsed.subCommands.isEmpty) { parsed } else {
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
    command.validate().foreach(logAndThrowError)
    command
  }

  def usage(jc: JCommander): String = {
    val out = new StringBuilder()
    out.append(s"Usage: $name [command] [command options]\n")
    val commands = jc.getCommands.asScala.keys.toSeq.sorted
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
    val commands = jc.getCommands.asScala.keys.toSeq
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
      val params = jc.getCommands.get(command).getParameters.asScala.filter(!_.getParameter.hidden()).flatMap(_.getParameter.names().filter(_.length != 2))
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

  private def getEnvironmentErrors: Option[String] = {
    val env = classpathEnvironments
    if (env.forall(sys.env.contains)) { None } else {
      val envMsg = if (env.size == 1) { "is not set as an environment variable" } else { "are not set as environment variables" }
      val types = env.map(e => e.take(1) ++ e.drop(1).toLowerCase(Locale.US).replace("_home", "")).mkString(" and ")
      val msg =
        s"\nWarning: ${env.mkString(" and/or ")} $envMsg." +
            s"\nGeoMesa tools will not run without the appropriate $types JARs in the tools classpath." +
            s"\nPlease ensure that those JARs are present in the classpath by running '$name classpath'." +
            "\nTo take corrective action, copy the necessary JAR files in the GeoMesa tools lib directory " +
            "using the provided 'install-dependencies.sh' and 'install-*-support.sh' scripts."
      Some(msg)
    }
  }

  class DefaultCommand(jc: JCommander) extends Command {
    override def execute(): Unit = Command.user.info(usage(jc))
    override val name: String = ""
    override val params: Any = null
  }

  class MainExecutor(args: Array[String]) extends Executor {
    override def execute(): Unit = parseCommand(args).execute()
  }

  class NailgunExecutor(context: NGContext) extends Executor {
    override def execute(): Unit = {
      val command = parseCommand(context.getArgs)
      context.getNGServer match {
        case ng: NailgunServer => ng.execute(command)
        case ng => throw new IllegalStateException(s"Expected a NailgunServer but got: $ng")
      }
    }
  }
}

object Runner {

  trait Executor {
    def execute(): Unit
  }

  /**
   * Result of executing a command
   *
   * @param code exit code
   * @param errors error messages or exceptions with full stack traces
   */
  case class CommandResult(code: Int, errors: Seq[Either[String, Throwable]] = Seq.empty)

  case class AutocompleteInfo(path: String, commandName: String)
}
