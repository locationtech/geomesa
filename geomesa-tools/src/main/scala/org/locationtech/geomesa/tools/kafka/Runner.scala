package org.locationtech.geomesa.tools.kafka

import com.beust.jcommander.{JCommander, ParameterException}
import com.typesafe.scalalogging.LazyLogging
import org.locationtech.geomesa.tools.accumulo.commands.convert.GeoMesaIStringConverterFactory
import org.locationtech.geomesa.tools.kafka.commands.{Command, CreateCommand, HelpCommand}

import scala.collection.JavaConversions._

object Runner extends LazyLogging {

  def main(args: Array[String]): Unit = {
    val command = createCommand(args)
    try {
      command.execute()
    } catch {
      case e: Exception =>
        logger.error(e.getMessage, e)
        sys.exit(-1)
    }
    sys.exit(0)
  }

  def createCommand(args: Array[String]): Command = {
    val jc = new JCommander()
    jc.setProgramName("geomesa-kafka")
    jc.addConverterFactory(new GeoMesaIStringConverterFactory)

    val commands = List(
      new CreateCommand(jc),
      new HelpCommand(jc)
    )

    commands.foreach(_.register)
    val commandMap = commands.map(c => c.command -> c).toMap

    try {
      jc.parse(args.toArray: _*)
    } catch {
      case pe: ParameterException =>
        println("Error parsing arguments: " + pe.getMessage)
        println(commandUsage(jc))
        sys.exit(-1)
    }

    commandMap.getOrElse(jc.getParsedCommand, new DefaultCommand(jc))
  }

  def mkSubCommand(parent: JCommander, name: String, obj: Object): JCommander = {
    parent.addCommand(name, obj)
    parent.getCommands().get(name)
  }

  class DefaultCommand(jc: JCommander) extends Command(jc) {
    override def execute() = println(commandUsage(jc))
    override def register = {}
    override val command: String = ""
    override val params: Any = null
  }

  def commandUsage(jc: JCommander) = {
    val out = new StringBuilder()
    out.append("Usage: geomesa-kafka [command] [command options]\n")
    out.append("  Commands:\n")
    val maxLen = jc.getCommands.map(_._1).map(_.length).max + 4
    jc.getCommands.map(_._1).toSeq.sorted.foreach { command =>
      val spaces = " " * (maxLen - command.length)
      out.append(s"    $command$spaces${jc.getCommandDescription(command)}\n")
    }
    out.toString()
  }
}
