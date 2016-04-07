package org.locationtech.geomesa.tools.kafka

import org.locationtech.geomesa.tools.common.Runner
import org.locationtech.geomesa.tools.common.commands.{Command, HelpCommand, VersionCommand}
import org.locationtech.geomesa.tools.kafka.commands.CreateCommand

object KafkaRunner extends Runner {
  override val scriptName: String = "geomesa-kafka"
  override val commands: List[Command] = List(
    new CreateCommand(jc),
    new HelpCommand(jc),
    new VersionCommand(jc)
  )

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
}
