/***********************************************************************
 * Copyright (c) 2013-2024 Commonwealth Computer Research, Inc.
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

  override protected def classpathEnvironments: Seq[String] = Seq("CASSANDRA_HOME")
}
