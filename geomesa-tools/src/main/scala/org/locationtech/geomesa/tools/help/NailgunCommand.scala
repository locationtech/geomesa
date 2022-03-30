/***********************************************************************
 * Copyright (c) 2013-2022 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.tools.help

import java.util.concurrent.TimeUnit

import com.beust.jcommander.Parameters
import org.locationtech.geomesa.tools.help.NailgunCommand._
import org.locationtech.geomesa.tools.utils.NailgunServer.NailgunAware
import org.locationtech.geomesa.tools.{Command, CommandWithSubCommands}
import org.locationtech.geomesa.utils.text.TextTools

/**
 * Note: most of this class is a placeholder for the 'ng' functions implemented in the 'geomesa-*' script,
 * to get it to show up in the JCommander help. The stats command is implemented here
 */
class NailgunCommand extends CommandWithSubCommands {
  override val name: String = "ng"
  override val params: NailgunParams = new NailgunParams()
  override val subCommands: Seq[Command] =
    Seq(new NailgunStartCommand(), new NailgunStopCommand(), new NailgunStatsCommand(), new NailgunClasspathCommand())
}

object NailgunCommand {

  @Parameters(commandDescription = "Manage the Nailgun server used for executing commands")
  class NailgunParams {}

  @Parameters(commandDescription = "Display stats from the Nailgun server used for executing commands")
  class NailgunStatsParams {}

  class NailgunStatsCommand extends Command with NailgunAware {

    import NailgunStatsCommand.{Header, toSeconds}

    override val name: String = "stats"
    override val params: NailgunStatsParams = new NailgunStatsParams()

    override def execute(): Unit = {
      nailgun match {
        case None => Command.output.info("Nailgun server not enabled")
        case Some(ng) =>
          val widths = Header.map(_.length).toArray
          val stats = ng.stats.map { stat =>
              val strings =
                Seq(
                  stat.name,
                  s"${stat.active}",
                  s"${stat.complete}",
                  f"${toSeconds(stat.mean)}%2.2f",
                  f"${toSeconds(stat.median)}%2.2f",
                  f"${toSeconds(stat.n95)}%2.2f",
                  f"${stat.rate * 60}%2.2f"
                )
            var i = 0
            while (i < widths.length) {
              widths(i) = math.max(widths(i), strings(i).length)
              i += 1
            }
            strings
          }

          def pad(seq: Seq[String]): String = {
            var i = 0
            val padded = seq.map { s =>
              val p = s.padTo(widths(i), ' ')
              i += 1
              p
            }
            padded.mkString(" ")
          }

          Command.output.info(s"Uptime ${TextTools.getTime(ng.started)}\n")
          Command.output.info(pad(Header))
          stats.sortBy(_.head).foreach(s => Command.output.info(pad(s)))
      }
    }
  }

  object NailgunStatsCommand {

    val Header = Seq("Command", "Active", "Complete", "Average", "Median", "95%", "Rate/min")
    val DurationFactor: Long = TimeUnit.SECONDS.toNanos(1)

    def toSeconds(duration: Double): Double = duration / DurationFactor
  }

  @Parameters(commandDescription = "Start the Nailgun server used for executing commands")
  class NailgunStartParams {}

  class NailgunStartCommand extends Command {
    override val name: String = "start"
    override val params: NailgunStartParams = new NailgunStartParams()
    override def execute(): Unit = {}
  }

  @Parameters(commandDescription = "Stop the Nailgun server used for executing commands")
  class NailgunStopParams {}

  class NailgunStopCommand extends Command {
    override val name: String = "stop"
    override val params: NailgunStopParams = new NailgunStopParams()
    override def execute(): Unit = {}
  }

  @Parameters(commandDescription = "Displays the classpath of the Nailgun server used for executing commands")
  class NailgunClasspathParams {}

  class NailgunClasspathCommand extends Command {
    override val name: String = "classpath"
    override val params: NailgunClasspathParams = new NailgunClasspathParams()
    override def execute(): Unit = {}
  }
}

