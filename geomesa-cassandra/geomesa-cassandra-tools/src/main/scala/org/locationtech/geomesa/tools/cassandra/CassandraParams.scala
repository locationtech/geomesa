package org.locationtech.geomesa.tools.cassandra

import com.beust.jcommander.Parameter

/**
  * Created by etalbot on 10/18/16.
  */
trait CassandraConnectionParams {
  @Parameter(names = Array("--contact-point"), description = "contact-point", required = true)
  var contactPoint: String = null

  @Parameter(names = Array("--key-space"), description = "key-space", required = true)
  var keySpace: String = null

  @Parameter(names = Array("--name-space"), description = "name-space", required = true)
  var nameSpace: String = null
}
