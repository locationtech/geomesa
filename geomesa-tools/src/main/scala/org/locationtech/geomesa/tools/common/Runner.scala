/***********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.tools.common

import com.beust.jcommander.{JCommander, ParameterException}
import com.typesafe.scalalogging.LazyLogging
import org.locationtech.geomesa.tools.common.commands.Command
import org.locationtech.geomesa.tools.common.commands.convert.GeoMesaIStringConverterFactory

import scala.collection.JavaConversions._

trait Runner extends LazyLogging {
  val jc = new JCommander()
  val scriptName: String
  val commands: List[Command]

  def main(args: Array[String]): Unit = {
    val command = createCommand(args)
    try {
      command.execute()
    } catch {
      case e: IllegalArgumentException =>
        logger.error(e.getMessage)
        sys.exit(-1)
      case e: Exception =>
        logger.error(e.getMessage, e)
        sys.exit(-1)
    }
    sys.exit(0)
  }

  def createCommand(args: Array[String]): Command = {
    jc.setProgramName(scriptName)
    jc.addConverterFactory(new GeoMesaIStringConverterFactory)

    commands.foreach(_.register)

    try {
      jc.parse(args: _*)
    } catch {
      case pe: ParameterException =>
        println("Error parsing arguments: " + pe.getMessage)
        val usage = Option(jc.getCommands.get(jc.getParsedCommand)).map { c =>
          val out = new java.lang.StringBuilder
          c.usage(out)
          out.toString
        }.getOrElse(commandUsage(jc))
        println
        println(usage)
        sys.exit(-1)
    }

    val command = commands.find(_.command == jc.getParsedCommand).getOrElse(new DefaultCommand(jc))
    resolveEnvironment(command)
    command
  }

  def resolveEnvironment(command: Command): Unit = {}

  def mkSubCommand(parent: JCommander, name: String, obj: Object): JCommander = {
    parent.addCommand(name, obj)
    parent.getCommands.get(name)
  }

  class DefaultCommand(jc: JCommander) extends Command(jc) {
    override def execute() = println(commandUsage(jc))
    override def register = {}
    override val command: String = ""
    override val params: Any = null
  }

  def commandUsage(jc: JCommander) = {
    val out = new StringBuilder()
    out.append(s"Usage: $scriptName [command] [command options]\n")
    out.append("  Commands:\n")
    val maxLen = jc.getCommands.map(_._1).map(_.length).max + 4
    jc.getCommands.map(_._1).toSeq.sorted.foreach { command =>
      val spaces = " " * (maxLen - command.length)
      out.append(s"    $command$spaces${jc.getCommandDescription(command)}\n")
    }
    out.toString()
  }
}
