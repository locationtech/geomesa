/***********************************************************************
 * Copyright (c) 2013-2022 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.cassandra.tools

import org.locationtech.geomesa.cassandra.tools.commands._
import org.locationtech.geomesa.cassandra.tools.export.{CassandraExportCommand, CassandraPlaybackCommand}
import org.locationtech.geomesa.tools.{Command, Runner}

object CassandraRunner extends Runner {

  override val name: String = "geomesa-cassandra"

  override protected def commands: Seq[Command] = {
    super.commands ++ Seq(
      new CassandraGetTypeNamesCommand,
      new CassandraDescribeSchemaCommand,
      new CassandraGetSftConfigCommand,
      new CassandraCreateSchemaCommand,
      new CassandraRemoveSchemaCommand,
      new CassandraDeleteFeaturesCommand,
      new CassandraIngestCommand,
      new CassandraExportCommand,
      new CassandraPlaybackCommand,
      new CassandraExplainCommand,
      new CassandraUpdateSchemaCommand
    )
  }

  override def environmentErrorInfo(): Option[String] = {
    if (!sys.env.contains("CASSANDRA_HOME")) {
      Option("Warning: you have not set the CASSANDRA_HOME environment variable." +
        "\nGeoMesa tools will not run without the appropriate Cassandra jars on the classpath.")
    } else { None }
  }
}
