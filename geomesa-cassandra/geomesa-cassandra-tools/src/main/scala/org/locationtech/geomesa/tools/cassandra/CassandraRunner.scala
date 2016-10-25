/***********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.tools.cassandra

import org.locationtech.geomesa.tools.cassandra.commands.{CassandraDescribeCommand, CassandraListCommand}
import org.locationtech.geomesa.tools.common.Runner
import org.locationtech.geomesa.tools.common.commands.Command


object CassandraRunner extends Runner {

  override val scriptName: String = "geomesa-cassandra"

  override val commands: List[Command] = List(
    new CassandraListCommand(jc),
    new CassandraDescribeCommand(jc)
  )
}
