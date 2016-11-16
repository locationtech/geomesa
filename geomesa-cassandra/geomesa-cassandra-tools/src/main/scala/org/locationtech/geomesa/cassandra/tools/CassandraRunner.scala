/***********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.cassandra.tools

import com.beust.jcommander.JCommander
import org.locationtech.geomesa.cassandra.tools.commands.{CassandraDescribeSchemaCommand, CassandraGetTypeNamesCommand}
import org.locationtech.geomesa.tools.{Command, Runner}


object CassandraRunner extends Runner {

  override val name: String = "geomesa-cassandra"

  override def createCommands(jc: JCommander): Seq[Command] = Seq(
    new CassandraGetTypeNamesCommand,
    new CassandraDescribeSchemaCommand
  )
}
