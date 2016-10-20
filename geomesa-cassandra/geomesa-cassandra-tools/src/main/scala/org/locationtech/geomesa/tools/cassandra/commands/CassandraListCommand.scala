package org.locationtech.geomesa.tools.cassandra.commands

import com.beust.jcommander.{JCommander, Parameters}
import com.typesafe.scalalogging.LazyLogging
import org.locationtech.geomesa.tools.cassandra.CassandraConnectionParams
import org.locationtech.geomesa.tools.cassandra.commands.CassandraListCommand.CassandraListParameters
import org.locationtech.geomesa.tools.common.commands.{Command, ListCommand}


class CassandraListCommand(parent: JCommander)
  extends Command(parent)
    with ListCommand
    with CommandWithCassandraDataStore
    with LazyLogging {

  override val params = new CassandraListParameters()

}

object CassandraListCommand {
  @Parameters(commandDescription = "List GeoMesa feature types for a given catalog")
  class CassandraListParameters extends CassandraConnectionParams {}
}