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
