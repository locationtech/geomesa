package org.locationtech.geomesa.tools.cassandra.commands

import com.beust.jcommander.{JCommander, Parameters}
import com.typesafe.scalalogging.LazyLogging
import org.locationtech.geomesa.tools.cassandra.CassandraConnectionParams
import org.locationtech.geomesa.tools.cassandra.commands.CassandraDescribeCommand.CassandraDescribeParameters
import org.locationtech.geomesa.tools.common.FeatureTypeNameParam
import org.locationtech.geomesa.tools.common.commands.{Command, DescribeCommand}


class CassandraDescribeCommand(parent: JCommander)
  extends Command(parent)
    with DescribeCommand
    with CommandWithCassandraDataStore
    with LazyLogging {

  override val params = new CassandraDescribeParameters {}

}

object CassandraDescribeCommand {
  @Parameters(commandDescription = "describe")
  class CassandraDescribeParameters extends FeatureTypeNameParam with CassandraConnectionParams {}
}