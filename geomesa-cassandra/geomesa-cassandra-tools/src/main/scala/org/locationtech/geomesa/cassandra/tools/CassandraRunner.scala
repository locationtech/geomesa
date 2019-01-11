/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.cassandra.tools

import com.beust.jcommander.JCommander
import org.locationtech.geomesa.cassandra.tools.commands._
import org.locationtech.geomesa.cassandra.tools.export.{CassandraExportCommand, CassandraPlaybackCommand}
import org.locationtech.geomesa.tools.status._
import org.locationtech.geomesa.tools.{Command, Runner}

object CassandraRunner extends Runner {

  override val name: String = "geomesa-cassandra"

  override def createCommands(jc: JCommander): Seq[Command] = Seq(
    new CassandraGetTypeNamesCommand,
    new CassandraDescribeSchemaCommand,
    new HelpCommand(this, jc),
    new EnvironmentCommand,
    new VersionCommand,
    new CassandraGetSftConfigCommand,
    new CassandraCreateSchemaCommand,
    new CassandraRemoveSchemaCommand,
    new CassandraDeleteFeaturesCommand,
    new CassandraIngestCommand,
    new CassandraExportCommand,
    new CassandraPlaybackCommand,
    new CassandraExplainCommand,
    new ConfigureCommand,
    new ClasspathCommand,
    new ScalaConsoleCommand
  )

  override def environmentErrorInfo(): Option[String] = {
    if (sys.env.get("CASSANDRA_HOME").isEmpty) {
      Option("Warning: you have not set the CASSANDRA_HOME environment variable." +
        "\nGeoMesa tools will not run without the appropriate Cassandra jars on the classpath.")
    } else { None }
  }
}
